/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.SourceType;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionWorkspaceGrantRecord;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.JSONB;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SourceServiceJooqImpl implements SourceService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ExceptionWrappingDatabase database;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final ConnectionService connectionService;
  private final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;

  // TODO: This has too many dependencies.
  public SourceServiceJooqImpl(@Named("configDatabase") final Database database,
                               final FeatureFlagClient featureFlagClient,
                               final SecretsRepositoryReader secretsRepositoryReader,
                               final SecretsRepositoryWriter secretsRepositoryWriter,
                               final SecretPersistenceConfigService secretPersistenceConfigService,
                               final ConnectionService connectionService,
                               final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater) {
    this.database = new ExceptionWrappingDatabase(database);
    this.connectionService = connectionService;
    this.featureFlagClient = featureFlagClient;
    this.secretRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater;
  }

  /**
   * Get source definition.
   *
   * @param sourceDefinitionId source definition id
   * @return source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Override
  public StandardSourceDefinition getStandardSourceDefinition(final UUID sourceDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return sourceDefQuery(Optional.of(sourceDefinitionId), true)
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_SOURCE_DEFINITION, sourceDefinitionId));
  }

  /**
   * Get source definition form source.
   *
   * @param sourceId source id
   * @return source definition
   */
  @Override
  public StandardSourceDefinition getSourceDefinitionFromSource(final UUID sourceId) {
    try {
      final SourceConnection source = getSourceConnection(sourceId);
      return getStandardSourceDefinition(source.getSourceDefinitionId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get source definition used by a connection.
   *
   * @param connectionId connection id
   * @return source definition
   */
  @Override
  public StandardSourceDefinition getSourceDefinitionFromConnection(final UUID connectionId) {
    try {
      // TODO: This should be refactored to use the repository. Services should not depend on other
      // services.
      final StandardSync sync = connectionService.getStandardSync(connectionId);
      return getSourceDefinitionFromSource(sync.getSourceId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * List standard source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return list source definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardSourceDefinition> listStandardSourceDefinitions(final boolean includeTombstone)
      throws IOException {
    return sourceDefQuery(Optional.empty(), includeTombstone).toList();
  }

  /**
   * List public source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return public source definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardSourceDefinition> listPublicSourceDefinitions(final boolean includeTombstone)
      throws IOException {
    return listStandardActorDefinitions(
        ActorType.source,
        record -> DbConverter.buildStandardSourceDefinition(record, retrieveDefaultMaxSecondsBetweenMessages(record.get(ACTOR_DEFINITION.ID))),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstone),
        ACTOR_DEFINITION.PUBLIC.eq(true));
  }

  /**
   * List granted source definitions for workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned destinations
   * @return list standard source definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardSourceDefinition> listGrantedSourceDefinitions(final UUID workspaceId,
                                                                     final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace,
        JoinType.JOIN,
        ActorType.source,
        record -> DbConverter.buildStandardSourceDefinition(record, retrieveDefaultMaxSecondsBetweenMessages(record.get(ACTOR_DEFINITION.ID))),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * List source to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned definitions
   * @return list of pairs from source definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<Entry<StandardSourceDefinition, Boolean>> listGrantableSourceDefinitions(
                                                                                       final UUID workspaceId,
                                                                                       final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace,
        JoinType.LEFT_OUTER_JOIN,
        ActorType.source,
        record -> actorDefinitionWithGrantStatus(record,
            entry -> DbConverter.buildStandardSourceDefinition(entry, retrieveDefaultMaxSecondsBetweenMessages(record.get(ACTOR_DEFINITION.ID)))),
        ACTOR_DEFINITION.CUSTOM.eq(false),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * Update source definition.
   *
   * @param sourceDefinition source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   */
  @Override
  public void updateStandardSourceDefinition(final StandardSourceDefinition sourceDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // Check existence before updating
    // TODO: split out write and update methods so that we don't need explicit checking
    getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());

    database.transaction(ctx -> {
      writeStandardSourceDefinition(Collections.singletonList(sourceDefinition), ctx);
      return null;
    });
  }

  /**
   * Returns source with a given id. Does not contain secrets. To hydrate with secrets see { @link
   * SecretsRepositoryReader#getSourceConnectionWithSecrets(final UUID sourceId) }.
   *
   * @param sourceId - id of source to fetch.
   * @return sources
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Override
  public SourceConnection getSourceConnection(final UUID sourceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    return listSourceQuery(Optional.of(sourceId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.SOURCE_CONNECTION, sourceId));
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }
   * <p>
   * Write a SourceConnection to the database. The configuration of the Source will be a partial
   * configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialSource - The configuration of the Source will be a partial configuration (no
   *        secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeSourceConnectionNoSecrets(final SourceConnection partialSource) throws IOException {
    database.transaction(ctx -> {
      writeSourceConnection(Collections.singletonList(partialSource), ctx);
      return null;
    });
  }

  /**
   * Returns all sources in the database. Does not contain secrets.
   *
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<SourceConnection> listSourceConnection() throws IOException {
    return listSourceQuery(Optional.empty()).toList();
  }

  /**
   * Returns all sources for a workspace. Does not contain secrets.
   *
   * @param workspaceId - id of the workspace
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<SourceConnection> listWorkspaceSourceConnection(final UUID workspaceId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildSourceConnection).collect(Collectors.toList());
  }

  /**
   * Returns if a source is active, i.e. the source has at least one active or manual connection.
   *
   * @param sourceId - id of the source
   * @return boolean - if source is active or not
   * @throws IOException - you never know when you IO
   */
  @Override
  public Boolean isSourceActive(final UUID sourceId) throws IOException {
    return database.query(ctx -> ctx.fetchExists(select()
        .from(CONNECTION)
        .where(CONNECTION.SOURCE_ID.eq(sourceId))
        .and(CONNECTION.STATUS.eq(StatusType.active))));
  }

  /**
   * Returns all sources for a set of workspaces. Does not contain secrets.
   *
   * @param resourcesQueryPaginated - Includes all the things we might want to query
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<SourceConnection> listWorkspacesSourceConnections(
                                                                final ResourcesQueryPaginated resourcesQueryPaginated)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.WORKSPACE_ID.in(resourcesQueryPaginated.workspaceIds()))
        .and(resourcesQueryPaginated.includeDeleted() ? noCondition() : ACTOR.TOMBSTONE.notEqual(true))
        .limit(resourcesQueryPaginated.pageSize())
        .offset(resourcesQueryPaginated.rowOffset())
        .fetch());
    return result.stream().map(DbConverter::buildSourceConnection).collect(Collectors.toList());
  }

  /**
   * Returns all active sources using a definition.
   *
   * @param definitionId - id for the definition
   * @return sources
   * @throws IOException - exception while interacting with the db
   */
  @Override
  public List<SourceConnection> listSourcesForDefinition(final UUID definitionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildSourceConnection).collect(Collectors.toList());
  }

  /**
   * Get source and definition from sources ids.
   *
   * @param sourceIds source ids
   * @return pair of source and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<SourceAndDefinition> getSourceAndDefinitionsFromSourceIds(final List<UUID> sourceIds)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx
        .select(ACTOR.asterisk(), ACTOR_DEFINITION.asterisk())
        .from(ACTOR)
        .join(ACTOR_DEFINITION)
        .on(ACTOR.ACTOR_DEFINITION_ID.eq(ACTOR_DEFINITION.ID))
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source), ACTOR.ID.in(sourceIds))
        .fetch());

    final List<SourceAndDefinition> sourceAndDefinitions = new ArrayList<>();

    for (final Record record : records) {
      final SourceConnection source = DbConverter.buildSourceConnection(record);
      final StandardSourceDefinition definition = DbConverter.buildStandardSourceDefinition(record,
          retrieveDefaultMaxSecondsBetweenMessages(source.getSourceDefinitionId()));
      sourceAndDefinitions.add(new SourceAndDefinition(source, definition));
    }

    return sourceAndDefinitions;
  }

  /**
   * Write metadata for a source connector. Writes global metadata (source definition, breaking
   * changes) and versioned metadata (info for actor definition version to set as default). Sets the
   * new version as the default version and updates actors accordingly, based on whether the upgrade
   * will be breaking or not.
   *
   * @param sourceDefinition standard source definition
   * @param actorDefinitionVersion actor definition version, containing tag to set as default
   * @param breakingChangesForDefinition - list of breaking changes for the definition
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion,
                                     final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException {
    database.transaction(ctx -> {
      writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, breakingChangesForDefinition, ctx);
      return null;
    });

    // FIXME(pedro): this should be moved out of this service
    actorDefinitionVersionUpdater.updateSourceDefaultVersion(sourceDefinition, actorDefinitionVersion, breakingChangesForDefinition);
  }

  @Override
  public void writeCustomConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                           final ActorDefinitionVersion defaultVersion,
                                           final UUID scopeId,
                                           final ScopeType scopeType)
      throws IOException {
    database.transaction(ctx -> {
      writeConnectorMetadata(sourceDefinition, defaultVersion, List.of(), ctx);
      writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), scopeId,
          io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.toString()), ctx);
      return null;
    });

    actorDefinitionVersionUpdater.updateSourceDefaultVersion(sourceDefinition, defaultVersion, List.of());
  }

  @Override
  public List<SourceConnection> listSourcesWithIds(final List<UUID> sourceIds) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.ID.in(sourceIds))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildSourceConnection).toList();
  }

  /**
   * Retrieve from Launch Darkly the default max seconds between messages for a given source. This
   * allows us to dynamically change the default max seconds between messages for a source.
   *
   * @param sourceDefinitionId to retrieve the default max seconds between messages for.
   * @return
   */
  private Long retrieveDefaultMaxSecondsBetweenMessages(final UUID sourceDefinitionId) {
    return Long.parseLong(featureFlagClient.stringVariation(HeartbeatMaxSecondsBetweenMessages.INSTANCE, new SourceDefinition(sourceDefinitionId)));
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

  private void writeConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                      final ActorDefinitionVersion actorDefinitionVersion,
                                      final List<ActorDefinitionBreakingChange> breakingChangesForDefinition,
                                      final DSLContext ctx) {
    writeStandardSourceDefinition(Collections.singletonList(sourceDefinition), ctx);
    ConnectorMetadataJooqHelper.writeActorDefinitionBreakingChanges(breakingChangesForDefinition, ctx);
    ConnectorMetadataJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx);
  }

  private Stream<StandardSourceDefinition> sourceDefQuery(final Optional<UUID> sourceDefId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.source))
        .and(sourceDefId.map(ACTOR_DEFINITION.ID::eq).orElse(noCondition()))
        .and(includeTombstone ? noCondition() : ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
        .fetch())
        .stream()
        .map(record -> DbConverter.buildStandardSourceDefinition(record, retrieveDefaultMaxSecondsBetweenMessages(sourceDefId.orElse(ANONYMOUS))));
  }

  private <T> List<T> listStandardActorDefinitions(final ActorType actorType,
                                                   final Function<Record, T> recordToActorDefinition,
                                                   final Condition... conditions)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(asterisk()).from(ACTOR_DEFINITION)
        .where(conditions)
        .and(ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType))
        .fetch());

    return records.stream()
        .map(recordToActorDefinition)
        .toList();
  }

  private <T> List<T> listActorDefinitionsJoinedWithGrants(final UUID scopeId,
                                                           final io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType scopeType,
                                                           final JoinType joinType,
                                                           final ActorType actorType,
                                                           final Function<Record, T> recordToReturnType,
                                                           final Condition... conditions)
      throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        scopeId,
        scopeType,
        joinType,
        ArrayUtils.addAll(conditions,
            ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType),
            ACTOR_DEFINITION.PUBLIC.eq(false)));

    return records.stream()
        .map(recordToReturnType)
        .toList();
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

  private Optional<UUID> getOrganizationIdFromWorkspaceId(final UUID scopeId) throws IOException {
    final Optional<Record1<UUID>> optionalRecord = database.query(ctx -> ctx.select(WORKSPACE.ORGANIZATION_ID).from(WORKSPACE)
        .where(WORKSPACE.ID.eq(scopeId)).fetchOptional());
    return optionalRecord.map(Record1::value1);
  }

  private <T> Entry<T, Boolean> actorDefinitionWithGrantStatus(final Record outerJoinRecord,
                                                               final Function<Record, T> recordToActorDefinition) {
    final T actorDefinition = recordToActorDefinition.apply(outerJoinRecord);
    final boolean granted = outerJoinRecord.get(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID) != null;
    return Map.entry(actorDefinition, granted);
  }

  private static void writeStandardSourceDefinition(final List<StandardSourceDefinition> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((standardSourceDefinition) -> {
      final boolean isExistingConfig = ctx.fetchExists(DSL.select()
          .from(Tables.ACTOR_DEFINITION)
          .where(Tables.ACTOR_DEFINITION.ID.eq(standardSourceDefinition.getSourceDefinitionId())));

      if (isExistingConfig) {
        ctx.update(Tables.ACTOR_DEFINITION)
            .set(Tables.ACTOR_DEFINITION.ID, standardSourceDefinition.getSourceDefinitionId())
            .set(Tables.ACTOR_DEFINITION.NAME, standardSourceDefinition.getName())
            .set(Tables.ACTOR_DEFINITION.ICON, standardSourceDefinition.getIcon())
            .set(Tables.ACTOR_DEFINITION.ICON_URL, standardSourceDefinition.getIconUrl())
            .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR_DEFINITION.SOURCE_TYPE,
                standardSourceDefinition.getSourceType() == null ? null
                    : Enums.toEnum(standardSourceDefinition.getSourceType().value(),
                        SourceType.class).orElseThrow())
            .set(Tables.ACTOR_DEFINITION.TOMBSTONE, standardSourceDefinition.getTombstone())
            .set(Tables.ACTOR_DEFINITION.PUBLIC, standardSourceDefinition.getPublic())
            .set(Tables.ACTOR_DEFINITION.CUSTOM, standardSourceDefinition.getCustom())
            .set(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                standardSourceDefinition.getResourceRequirements() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getResourceRequirements())))
            .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
            .set(Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES,
                standardSourceDefinition.getMaxSecondsBetweenMessages() == null ? null
                    : standardSourceDefinition.getMaxSecondsBetweenMessages().intValue())
            .set(ACTOR_DEFINITION.METRICS,
                standardSourceDefinition.getMetrics() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getMetrics())))
            .where(Tables.ACTOR_DEFINITION.ID.eq(standardSourceDefinition.getSourceDefinitionId()))
            .execute();

      } else {
        ctx.insertInto(Tables.ACTOR_DEFINITION)
            .set(Tables.ACTOR_DEFINITION.ID, standardSourceDefinition.getSourceDefinitionId())
            .set(Tables.ACTOR_DEFINITION.NAME, standardSourceDefinition.getName())
            .set(Tables.ACTOR_DEFINITION.ICON, standardSourceDefinition.getIcon())
            .set(Tables.ACTOR_DEFINITION.ICON_URL, standardSourceDefinition.getIconUrl())
            .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR_DEFINITION.SOURCE_TYPE,
                standardSourceDefinition.getSourceType() == null ? null
                    : Enums.toEnum(standardSourceDefinition.getSourceType().value(),
                        SourceType.class).orElseThrow())
            .set(Tables.ACTOR_DEFINITION.TOMBSTONE, standardSourceDefinition.getTombstone() != null && standardSourceDefinition.getTombstone())
            .set(Tables.ACTOR_DEFINITION.PUBLIC, standardSourceDefinition.getPublic())
            .set(Tables.ACTOR_DEFINITION.CUSTOM, standardSourceDefinition.getCustom())
            .set(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                standardSourceDefinition.getResourceRequirements() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getResourceRequirements())))
            .set(Tables.ACTOR_DEFINITION.CREATED_AT, timestamp)
            .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
            .set(Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES,
                standardSourceDefinition.getMaxSecondsBetweenMessages() == null ? null
                    : standardSourceDefinition.getMaxSecondsBetweenMessages().intValue())
            .set(ACTOR_DEFINITION.METRICS,
                standardSourceDefinition.getMetrics() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getMetrics())))
            .execute();
      }
    });
  }

  private void writeSourceConnection(final List<SourceConnection> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((sourceConnection) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR)
          .where(ACTOR.ID.eq(sourceConnection.getSourceId())));

      if (isExistingConfig) {
        ctx.update(ACTOR)
            .set(ACTOR.ID, sourceConnection.getSourceId())
            .set(ACTOR.WORKSPACE_ID, sourceConnection.getWorkspaceId())
            .set(ACTOR.ACTOR_DEFINITION_ID, sourceConnection.getSourceDefinitionId())
            .set(ACTOR.NAME, sourceConnection.getName())
            .set(ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceConnection.getConfiguration())))
            .set(ACTOR.ACTOR_TYPE, ActorType.source)
            .set(ACTOR.TOMBSTONE, sourceConnection.getTombstone() != null && sourceConnection.getTombstone())
            .set(ACTOR.UPDATED_AT, timestamp)
            .where(ACTOR.ID.eq(sourceConnection.getSourceId()))
            .execute();
      } else {
        ctx.insertInto(ACTOR)
            .set(ACTOR.ID, sourceConnection.getSourceId())
            .set(ACTOR.WORKSPACE_ID, sourceConnection.getWorkspaceId())
            .set(ACTOR.ACTOR_DEFINITION_ID, sourceConnection.getSourceDefinitionId())
            .set(ACTOR.NAME, sourceConnection.getName())
            .set(ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceConnection.getConfiguration())))
            .set(ACTOR.ACTOR_TYPE, ActorType.source)
            .set(ACTOR.TOMBSTONE, sourceConnection.getTombstone() != null && sourceConnection.getTombstone())
            .set(ACTOR.CREATED_AT, timestamp)
            .set(ACTOR.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  private Stream<SourceConnection> listSourceQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR);
      if (configId.isPresent()) {
        return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.source), ACTOR.ID.eq(configId.get())).fetch();
      }
      return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.source)).fetch();
    });

    return result.map(DbConverter::buildSourceConnection).stream();
  }

  private Condition includeTombstones(final Field<Boolean> tombstoneField, final boolean includeTombstones) {
    if (includeTombstones) {
      return DSL.trueCondition();
    } else {
      return tombstoneField.eq(false);
    }
  }

  /**
   * Get source with secrets.
   *
   * @param sourceId source id
   * @return source with secrets
   */
  @Override
  public SourceConnection getSourceConnectionWithSecrets(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceConnection source = getSourceConnection(sourceId);
    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(source.getWorkspaceId());
    final JsonNode hydratedConfig;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get());
      hydratedConfig = secretRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(source.getConfiguration(),
          new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      hydratedConfig = secretRepositoryReader.hydrateConfigFromDefaultSecretPersistence(source.getConfiguration());
    }
    return Jsons.clone(source).withConfiguration(hydratedConfig);
  }

  /**
   * Delete source: tombstone source AND delete secrets
   *
   * @param name Source name
   * @param workspaceId workspace ID
   * @param sourceId source ID
   * @param spec spec for the destination
   * @throws JsonValidationException if the config is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Override
  public void tombstoneSource(
                              final String name,
                              final UUID workspaceId,
                              final UUID sourceId,
                              final ConnectorSpecification spec)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    // 1. Delete secrets from config
    final SourceConnection sourceConnection = getSourceConnection(sourceId);
    final JsonNode config = sourceConnection.getConfiguration();
    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(workspaceId);
    RuntimeSecretPersistence secretPersistence = null;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get());
      secretPersistence = new RuntimeSecretPersistence(secretPersistenceConfig);
    }
    secretsRepositoryWriter.deleteFromConfig(
        config,
        spec.getConnectionSpecification(),
        secretPersistence);

    // 2. Tombstone source and void config
    final SourceConnection newSourceConnection = new SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceConnection.getSourceDefinitionId())
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withConfiguration(null)
        .withTombstone(true);
    writeSourceConnectionNoSecrets(newSourceConnection);
  }

  /**
   * Write a source with its secrets to the appropriate persistence. Secrets go to secrets store and
   * the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param source to write
   * @param connectorSpecification spec for the destination
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Override
  public void writeSourceConnectionWithSecrets(
                                               final SourceConnection source,
                                               final ConnectorSpecification connectorSpecification)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final Optional<JsonNode> previousSourceConnection =
        getSourceIfExists(source.getSourceId()).map(SourceConnection::getConfiguration);

    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(source.getWorkspaceId());

    RuntimeSecretPersistence secretPersistence = null;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get());
      secretPersistence = new RuntimeSecretPersistence(secretPersistenceConfig);
    }
    final JsonNode partialConfig;
    if (previousSourceConnection.isPresent()) {
      partialConfig = secretsRepositoryWriter.updateFromConfig(
          source.getWorkspaceId(),
          previousSourceConnection.get(),
          source.getConfiguration(),
          connectorSpecification.getConnectionSpecification(), secretPersistence);
    } else {
      partialConfig = secretsRepositoryWriter.createFromConfig(
          source.getWorkspaceId(),
          source.getConfiguration(),
          connectorSpecification.getConnectionSpecification(),
          secretPersistence);
    }

    final SourceConnection partialSource = Jsons.clone(source).withConfiguration(partialConfig);
    writeSourceConnectionNoSecrets(partialSource);
  }

  private Optional<SourceConnection> getSourceIfExists(final UUID sourceId) {
    try {
      return Optional.of(getSourceConnection(sourceId));
    } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
      log.warn("Unable to find source with ID {}", sourceId);
      return Optional.empty();
    }
  }

}
