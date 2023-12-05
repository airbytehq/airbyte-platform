/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.asterisk;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ScopeType;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionWorkspaceGrantRecord;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.impl.DSL;

@Singleton
public class ActorDefinitionServiceJooqImpl implements ActorDefinitionService {

  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public ActorDefinitionServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Get actor definition IDs that are in use.
   *
   * @return list of IDs
   * @throws IOException - you never know when you IO
   */
  @Override
  public Set<UUID> getActorDefinitionIdsInUse() throws IOException {
    return database.query(ctx -> getActorDefinitionsInUse(ctx)
        .map(r -> r.get(ACTOR_DEFINITION.ID))
        .collect(Collectors.toSet()));
  }

  /**
   * Get actor definition ids to pair of actor type and protocol version.
   *
   * @return map of definition id to pair of actor type and protocol version.
   * @throws IOException - you never know when you IO
   */
  @Override
  public Map<UUID, Entry<ActorType, Version>> getActorDefinitionToProtocolVersionMap()
      throws IOException {
    return database.query(ctx -> getActorDefinitionsInUse(ctx)
        .collect(Collectors.toMap(r -> r.get(ACTOR_DEFINITION.ID),
            r -> Map.entry(
                r.get(ACTOR_DEFINITION.ACTOR_TYPE) == io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.source
                    ? io.airbyte.config.ActorType.SOURCE
                    : io.airbyte.config.ActorType.DESTINATION,
                AirbyteProtocolVersion.getWithDefault(r.get(ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION))),
            // We may have duplicated entries from the data. We can pick any values in the merge function
            (lhs, rhs) -> lhs)));
  }

  /**
   * Get a map of all actor definition ids and their default versions.
   *
   * @return map of definition id to default version.
   * @throws IOException - you never know when you IO
   */
  @Override
  public Map<UUID, ActorDefinitionVersion> getActorDefinitionIdsToDefaultVersionsMap()
      throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.ID, ACTOR_DEFINITION_VERSION.asterisk())
        .from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_VERSION)
        .on(ACTOR_DEFINITION.DEFAULT_VERSION_ID.eq(ACTOR_DEFINITION_VERSION.ID))
        .fetch()
        .stream()
        .collect(Collectors.toMap(
            record -> record.get(ACTOR_DEFINITION.ID),
            DbConverter::buildActorDefinitionVersion)));
  }

  /**
   * Update the docker image tag for multiple actor definitions at once.
   *
   * @param actorDefinitionIds the list of actor definition ids to update
   * @param targetImageTag the new docker image tag for these actor definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public int updateActorDefinitionsDockerImageTag(final List<UUID> actorDefinitionIds,
                                                  final String targetImageTag)
      throws IOException {
    return database.transaction(ctx -> writeSourceDefinitionImageTag(actorDefinitionIds, targetImageTag, ctx));
  }

  /**
   * Write actor definition workspace grant.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId workspace or organization id
   * @param scopeType ScopeType of either workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeActorDefinitionWorkspaceGrant(final UUID actorDefinitionId,
                                                 final UUID scopeId,
                                                 final ScopeType scopeType)
      throws IOException {
    database.query(ctx -> writeActorDefinitionWorkspaceGrant(actorDefinitionId, scopeId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.value()), ctx));
  }

  /**
   * Test if grant exists for actor definition and scope.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @return true, if the scope has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Override
  public boolean actorDefinitionWorkspaceGrantExists(final UUID actorDefinitionId,
                                                     final UUID scopeId,
                                                     final ScopeType scopeType)
      throws IOException {
    final Integer count = database.query(ctx -> ctx.fetchCount(
        DSL.selectFrom(ACTOR_DEFINITION_WORKSPACE_GRANT)
            .where(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .and(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId))
            .and(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
                io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.value())))));
    return count == 1;
  }

  /**
   * Delete workspace access to actor definition.
   *
   * @param actorDefinitionId actor definition id to remove
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Override
  public void deleteActorDefinitionWorkspaceGrant(final UUID actorDefinitionId,
                                                  final UUID scopeId,
                                                  final ScopeType scopeType)
      throws IOException {
    database.query(ctx -> ctx.deleteFrom(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .where(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .and(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId))
        .and(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
            io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.value())))
        .execute());
  }

  /**
   * Insert an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to insert
   * @throws IOException - you never know when you io
   * @returns the POJO associated with the actor definition version inserted. Contains the versionId
   *          field from the DB.
   */
  @Override
  public ActorDefinitionVersion writeActorDefinitionVersion(
                                                            final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    return database.transaction(ctx -> writeActorDefinitionVersion(actorDefinitionVersion, ctx));
  }

  /**
   * Get the actor definition version associated with an actor definition and a docker image tag.
   *
   * @param actorDefinitionId - actor definition id
   * @param dockerImageTag - docker image tag
   * @return actor definition version if there is an entry in the DB already for this version,
   *         otherwise an empty optional
   * @throws IOException - you never know when you io
   */
  @Override
  public Optional<ActorDefinitionVersion> getActorDefinitionVersion(final UUID actorDefinitionId,
                                                                    final String dockerImageTag)
      throws IOException {
    return database.query(ctx -> getActorDefinitionVersion(actorDefinitionId, dockerImageTag, ctx));
  }

  /**
   * Get an actor definition version by ID.
   *
   * @param actorDefinitionVersionId - actor definition version id
   * @return actor definition version
   * @throws ConfigNotFoundException if an actor definition version with the provided ID does not
   *         exist
   * @throws IOException - you never know when you io
   */
  @Override
  public ActorDefinitionVersion getActorDefinitionVersion(final UUID actorDefinitionVersionId)
      throws IOException, ConfigNotFoundException {
    return getActorDefinitionVersions(List.of(actorDefinitionVersionId))
        .stream()
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.ACTOR_DEFINITION_VERSION, actorDefinitionVersionId.toString()));
  }

  /**
   * List all actor definition versions for a given actor definition.
   *
   * @param actorDefinitionId - actor definition id
   * @return list of actor definition versions
   * @throws IOException - you never know when you io
   */
  @Override
  public List<ActorDefinitionVersion> listActorDefinitionVersionsForDefinition(
                                                                               final UUID actorDefinitionId)
      throws IOException {
    return database.query(ctx -> ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION)
        .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch()
        .stream()
        .map(DbConverter::buildActorDefinitionVersion)
        .collect(Collectors.toList()));
  }

  /**
   * Get actor definition versions by ID.
   *
   * @param actorDefinitionVersionIds - actor definition version ids
   * @return list of actor definition version
   * @throws IOException - you never know when you io
   */
  @Override
  public List<ActorDefinitionVersion> getActorDefinitionVersions(
                                                                 final List<UUID> actorDefinitionVersionIds)
      throws IOException {
    return database.query(ctx -> ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION))
        .where(Tables.ACTOR_DEFINITION_VERSION.ID.in(actorDefinitionVersionIds))
        .fetch()
        .stream()
        .map(DbConverter::buildActorDefinitionVersion)
        .collect(Collectors.toList());
  }

  /**
   * Set the default version for an actor.
   *
   * @param actorId - actor id
   * @param actorDefinitionVersionId - actor definition version id
   */
  @Override
  public void setActorDefaultVersion(final UUID actorId, final UUID actorDefinitionVersionId)
      throws IOException {
    database.query(ctx -> ctx.update(Tables.ACTOR)
        .set(Tables.ACTOR.DEFAULT_VERSION_ID, actorDefinitionVersionId)
        .set(Tables.ACTOR.UPDATED_AT, OffsetDateTime.now())
        .where(Tables.ACTOR.ID.eq(actorId))
        .execute());
  }

  /**
   * Get the list of breaking changes available affecting an actor definition.
   *
   * @param actorDefinitionId - actor definition id
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Override
  public List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinition(
                                                                                   final UUID actorDefinitionId)
      throws IOException {
    return database.query(ctx -> listBreakingChangesForActorDefinition(actorDefinitionId, ctx));
  }

  /**
   * Set the support state for a list of actor definition versions.
   *
   * @param actorDefinitionVersionIds - actor definition version ids to update
   * @param supportState - support state to update to
   * @throws IOException - you never know when you io
   */
  @Override
  public void setActorDefinitionVersionSupportStates(final List<UUID> actorDefinitionVersionIds,
                                                     final SupportState supportState)
      throws IOException {
    database.query(ctx -> ctx.update(Tables.ACTOR_DEFINITION_VERSION)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
            Enums.toEnum(supportState.value(), io.airbyte.db.instance.configs.jooq.generated.enums.SupportState.class).orElseThrow())
        .set(Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT, OffsetDateTime.now())
        .where(Tables.ACTOR_DEFINITION_VERSION.ID.in(actorDefinitionVersionIds))
        .execute());
  }

  /**
   * Get the list of breaking changes available affecting an actor definition version.
   * <p>
   * "Affecting" breaking changes are those between the provided version (non-inclusive) and the actor
   * definition default version (inclusive).
   *
   * @param actorDefinitionVersion - actor definition version
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Override
  public List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinitionVersion(
                                                                                          final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    final List<ActorDefinitionBreakingChange> breakingChanges = listBreakingChangesForActorDefinition(actorDefinitionVersion.getActorDefinitionId());
    if (breakingChanges.isEmpty()) {
      return List.of();
    }

    final Version currentVersion = new Version(actorDefinitionVersion.getDockerImageTag());
    final Version latestVersion =
        new Version(getDefaultVersionForActorDefinitionId(actorDefinitionVersion.getActorDefinitionId()).getDockerImageTag());

    return breakingChanges.stream()
        .filter(breakingChange -> breakingChange.getVersion().greaterThan(currentVersion)
            && latestVersion.greaterThanOrEqualTo(breakingChange.getVersion()))
        .sorted((v1, v2) -> v1.getVersion().versionCompareTo(v2.getVersion()))
        .toList();
  }

  /**
   * List all breaking changes.
   *
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Override
  public List<ActorDefinitionBreakingChange> listBreakingChanges() throws IOException {
    return database.query(ctx -> ctx.selectFrom(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
        .fetch()
        .stream()
        .map(DbConverter::buildActorDefinitionBreakingChange)
        .collect(Collectors.toList()));
  }

  /**
   * Test if workspace or organization id has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId id of the workspace or organization
   * @param scopeType enum of workspace or organization
   * @return true, if the workspace or organization has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Override
  public boolean scopeCanUseDefinition(final UUID actorDefinitionId,
                                       final UUID scopeId,
                                       final String scopeType)
      throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        scopeId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType),
        JoinType.LEFT_OUTER_JOIN,
        ACTOR_DEFINITION.ID.eq(actorDefinitionId),
        ACTOR_DEFINITION.PUBLIC.eq(true).or(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)));
    return records.isNotEmpty();
  }

  private Optional<UUID> getOrganizationIdFromWorkspaceId(final UUID scopeId) throws IOException {
    final Optional<Record1<UUID>> optionalRecord = database.query(ctx -> ctx.select(WORKSPACE.ORGANIZATION_ID).from(WORKSPACE)
        .where(WORKSPACE.ID.eq(scopeId)).fetchOptional());
    return optionalRecord.map(Record1::value1);
  }

  private Result<Record> actorDefinitionsJoinedWithGrants(final UUID scopeId,
                                                          final io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType scopeType,
                                                          final JoinType joinType,
                                                          final Condition... conditions)
      throws IOException {
    Condition scopeConditional = ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.toString())).and(
            ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId));

    // if scope type is workspace, get organization id as well and add that into OR conditional
    if (scopeType == io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace) {
      final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(scopeId);
      if (organizationId.isPresent()) {
        scopeConditional = scopeConditional.or(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
            io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.organization).and(
                ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(organizationId.get())));
      }
    }

    final Condition finalScopeConditional = scopeConditional;
    return database.query(ctx -> ctx.select(asterisk()).from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_WORKSPACE_GRANT, joinType)
        .on(ACTOR_DEFINITION.ID.eq(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID).and(finalScopeConditional))
        .where(conditions)
        .fetch());
  }

  private static Stream<Record4<UUID, String, io.airbyte.db.instance.configs.jooq.generated.enums.ActorType, String>> getActorDefinitionsInUse(final DSLContext ctx) {
    return ctx
        .selectDistinct(ACTOR_DEFINITION.ID, ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, ACTOR_DEFINITION.ACTOR_TYPE,
            ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION)
        .from(ACTOR_DEFINITION)
        .join(ACTOR).on(ACTOR.ACTOR_DEFINITION_ID.equal(ACTOR_DEFINITION.ID))
        .join(ACTOR_DEFINITION_VERSION).on(ACTOR_DEFINITION_VERSION.ID.equal(ACTOR_DEFINITION.DEFAULT_VERSION_ID))
        .fetch()
        .stream();
  }

  private int writeSourceDefinitionImageTag(final List<UUID> sourceDefinitionIds, final String targetImageTag, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    // We are updating the same version since connector builder projects have a different concept of
    // versioning
    return ctx.update(ACTOR_DEFINITION_VERSION).set(ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, targetImageTag)
        .set(ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
        .where(
            ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.in(sourceDefinitionIds).andNot(ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(targetImageTag)))
        .execute();
  }

  private int writeActorDefinitionWorkspaceGrant(final UUID actorDefinitionId,
                                                 final UUID scopeId,
                                                 final io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType scopeType,
                                                 final DSLContext ctx) {
    InsertSetMoreStep<ActorDefinitionWorkspaceGrantRecord> insertStep = ctx.insertInto(
        ACTOR_DEFINITION_WORKSPACE_GRANT)
        .set(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID, actorDefinitionId)
        .set(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE, scopeType)
        .set(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID, scopeId);
    // todo remove when we drop the workspace_id column
    if (scopeType == io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace) {
      insertStep = insertStep.set(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID, scopeId);
    }
    return insertStep.execute();
  }

  private List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinition(final UUID actorDefinitionId, final DSLContext ctx) {
    return ctx.selectFrom(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
        .where(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch()
        .stream()
        .map(DbConverter::buildActorDefinitionBreakingChange)
        .collect(Collectors.toList());
  }

  private ActorDefinitionVersion getDefaultVersionForActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    return database.query(ctx -> getDefaultVersionForActorDefinitionId(actorDefinitionId, ctx));
  }

  private ActorDefinitionVersion getDefaultVersionForActorDefinitionId(final UUID actorDefinitionId, final DSLContext ctx) {
    return getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId, ctx).orElseThrow();
  }

  /**
   * Get an optional ADV for an actor definition's default version. The optional will be empty if the
   * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
   * the case is if we are in the process of inserting and have already written the source definition,
   * but not yet set its default version.
   */
  private Optional<ActorDefinitionVersion> getDefaultVersionForActorDefinitionIdOptional(final UUID actorDefinitionId, final DSLContext ctx) {
    return ctx.select(Tables.ACTOR_DEFINITION_VERSION.asterisk())
        .from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_VERSION).on(Tables.ACTOR_DEFINITION_VERSION.ID.eq(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
        .where(ACTOR_DEFINITION.ID.eq(actorDefinitionId))
        .fetch()
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion);
  }

  /**
   * Insert an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to insert
   * @param ctx database context
   * @throws IOException - you never know when you io
   * @returns the POJO associated with the actor definition version inserted. Contains the versionId
   *          field from the DB.
   */
  private ActorDefinitionVersion writeActorDefinitionVersion(final ActorDefinitionVersion actorDefinitionVersion, final DSLContext ctx) {
    return ActorDefinitionVersionJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx);
  }

  /**
   * Get the actor definition version associated with an actor definition and a docker image tag.
   *
   * @param actorDefinitionId - actor definition id
   * @param dockerImageTag - docker image tag
   * @param ctx database context
   * @return actor definition version if there is an entry in the DB already for this version,
   *         otherwise an empty optional
   * @throws IOException - you never know when you io
   */
  private Optional<ActorDefinitionVersion> getActorDefinitionVersion(final UUID actorDefinitionId,
                                                                     final String dockerImageTag,
                                                                     final DSLContext ctx) {
    return ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION)
        .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(dockerImageTag)))
        .fetch()
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion);
  }

}
