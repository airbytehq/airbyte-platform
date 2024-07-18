/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.shared.DestinationAndDefinition;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionWorkspaceGrantRecord;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
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
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@Singleton
public class DestinationServiceJooqImpl implements DestinationService {

  private final ExceptionWrappingDatabase database;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final ConnectionService connectionService;
  private final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;

  @VisibleForTesting
  public DestinationServiceJooqImpl(@Named("configDatabase") final Database database,
                                    final FeatureFlagClient featureFlagClient,
                                    final SecretsRepositoryReader secretsRepositoryReader,
                                    final SecretsRepositoryWriter secretsRepositoryWriter,
                                    final SecretPersistenceConfigService secretPersistenceConfigService,
                                    final ConnectionService connectionService,
                                    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater) {
    this.database = new ExceptionWrappingDatabase(database);
    this.connectionService = connectionService;
    this.featureFlagClient = featureFlagClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater;
  }

  /**
   * Get destination definition.
   *
   * @param destinationDefinitionId destination definition id
   * @return destination definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Override
  public StandardDestinationDefinition getStandardDestinationDefinition(
                                                                        final UUID destinationDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return destDefQuery(Optional.of(destinationDefinitionId), true)
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_DESTINATION_DEFINITION, destinationDefinitionId));
  }

  /**
   * Get destination definition form destination.
   *
   * @param destinationId destination id
   * @return destination definition
   */
  @Override
  public StandardDestinationDefinition getDestinationDefinitionFromDestination(final UUID destinationId) {
    try {
      final DestinationConnection destination = getDestinationConnection(destinationId);
      return getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns if a destination is active, i.e. the destination has at least one active or manual
   * connection.
   *
   * @param destinationId - id of the destination
   * @return boolean - if destination is active or not
   * @throws IOException - you never know when you IO
   */
  @Override
  public Boolean isDestinationActive(final UUID destinationId) throws IOException {
    return database.query(ctx -> ctx.fetchExists(select()
        .from(CONNECTION)
        .where(CONNECTION.DESTINATION_ID.eq(destinationId))
        .and(CONNECTION.STATUS.eq(StatusType.active))));
  }

  /**
   * Get destination definition used by a connection.
   *
   * @param connectionId connection id
   * @return destination definition
   */
  @Override
  public StandardDestinationDefinition getDestinationDefinitionFromConnection(final UUID connectionId) {
    try {
      final StandardSync sync = connectionService.getStandardSync(connectionId);
      return getDestinationDefinitionFromDestination(sync.getDestinationId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * List standard destination definitions.
   *
   * @param includeTombstone include tombstoned destinations
   * @return list destination definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardDestinationDefinition> listStandardDestinationDefinitions(
                                                                                final boolean includeTombstone)
      throws IOException {
    return destDefQuery(Optional.empty(), includeTombstone).toList();
  }

  /**
   * List public destination definitions.
   *
   * @param includeTombstone include tombstoned destinations
   * @return public destination definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardDestinationDefinition> listPublicDestinationDefinitions(
                                                                              final boolean includeTombstone)
      throws IOException {
    return listStandardActorDefinitions(
        ActorType.destination,
        DbConverter::buildStandardDestinationDefinition,
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstone),
        ACTOR_DEFINITION.PUBLIC.eq(true));
  }

  /**
   * List granted destination definitions for workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned destinations
   * @return list standard destination definitions
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardDestinationDefinition> listGrantedDestinationDefinitions(final UUID workspaceId,
                                                                               final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace,
        JoinType.JOIN,
        ActorType.destination,
        DbConverter::buildStandardDestinationDefinition,
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * List destinations to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned definitions
   * @return list of pairs from destination definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<Entry<StandardDestinationDefinition, Boolean>> listGrantableDestinationDefinitions(
                                                                                                 final UUID workspaceId,
                                                                                                 final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace,
        JoinType.LEFT_OUTER_JOIN,
        ActorType.destination,
        record -> actorDefinitionWithGrantStatus(record, DbConverter::buildStandardDestinationDefinition),
        ACTOR_DEFINITION.CUSTOM.eq(false),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * Update destination definition.
   *
   * @param destinationDefinition destination definition
   * @throws IOException - you never know when you IO
   */
  @Override
  public void updateStandardDestinationDefinition(
                                                  final StandardDestinationDefinition destinationDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // Check existence before updating
    // TODO: split out write and update methods so that we don't need explicit checking
    getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());

    database.transaction(ctx -> {
      writeStandardDestinationDefinition(Collections.singletonList(destinationDefinition), ctx);
      return null;
    });
  }

  /**
   * Returns destination with a given id. Does not contain secrets.
   *
   * @param destinationId - id of destination to fetch.
   * @return destinations
   * @throws JsonValidationException - throws if returned destinations are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no destination with that id can be found.
   */
  @Override
  public DestinationConnection getDestinationConnection(final UUID destinationId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listDestinationQuery(Optional.of(destinationId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DESTINATION_CONNECTION, destinationId));
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }
   * <p>
   * Write a DestinationConnection to the database. The configuration of the Destination will be a
   * partial configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialDestination - The configuration of the Destination will be a partial configuration
   *        (no secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeDestinationConnectionNoSecrets(final DestinationConnection partialDestination)
      throws IOException {
    database.transaction(ctx -> {
      writeDestinationConnection(Collections.singletonList(partialDestination), ctx);
      return null;
    });
  }

  /**
   * Returns all destinations in the database. Does not contain secrets. To hydrate with secrets see
   * { @link SecretsRepositoryReader#listDestinationConnectionWithSecrets() }.
   *
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<DestinationConnection> listDestinationConnection() throws IOException {
    return listDestinationQuery(Optional.empty()).toList();
  }

  /**
   * Returns all destinations for a workspace. Does not contain secrets.
   *
   * @param workspaceId - id of the workspace
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<DestinationConnection> listWorkspaceDestinationConnection(final UUID workspaceId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .and(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildDestinationConnection).collect(Collectors.toList());
  }

  /**
   * Returns all destinations for a list of workspaces. Does not contain secrets.
   *
   * @param resourcesQueryPaginated - Includes all the things we might want to query
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<DestinationConnection> listWorkspacesDestinationConnections(
                                                                          final ResourcesQueryPaginated resourcesQueryPaginated)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .and(ACTOR.WORKSPACE_ID.in(resourcesQueryPaginated.workspaceIds()))
        .and(resourcesQueryPaginated.includeDeleted() ? noCondition() : ACTOR.TOMBSTONE.notEqual(true))
        .limit(resourcesQueryPaginated.pageSize())
        .offset(resourcesQueryPaginated.rowOffset())
        .fetch());
    return result.stream().map(DbConverter::buildDestinationConnection).collect(Collectors.toList());
  }

  /**
   * Returns all active destinations using a definition.
   *
   * @param definitionId - id for the definition
   * @return destinations
   * @throws IOException - exception while interacting with the db
   */
  @Override
  public List<DestinationConnection> listDestinationsForDefinition(final UUID definitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .and(ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildDestinationConnection).collect(Collectors.toList());
  }

  /**
   * Get destination and definition from destinations ids.
   *
   * @param destinationIds destination ids
   * @return pair of destination and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<DestinationAndDefinition> getDestinationAndDefinitionsFromDestinationIds(
                                                                                       final List<UUID> destinationIds)
      throws IOException {
    final Result<Record> records = database.query(ctx -> ctx
        .select(ACTOR.asterisk(), ACTOR_DEFINITION.asterisk())
        .from(ACTOR)
        .join(ACTOR_DEFINITION)
        .on(ACTOR.ACTOR_DEFINITION_ID.eq(ACTOR_DEFINITION.ID))
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination), ACTOR.ID.in(destinationIds))
        .fetch());

    final List<DestinationAndDefinition> destinationAndDefinitions = new ArrayList<>();

    for (final Record record : records) {
      final DestinationConnection destination = DbConverter.buildDestinationConnection(record);
      final StandardDestinationDefinition definition = DbConverter.buildStandardDestinationDefinition(record);
      destinationAndDefinitions.add(new DestinationAndDefinition(destination, definition));
    }

    return destinationAndDefinitions;
  }

  /**
   * Write metadata for a custom destination: global metadata (destination definition) and versioned
   * metadata (actor definition version for the version to use).
   *
   * @param destinationDefinition destination definition
   * @param defaultVersion default actor definition version
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeCustomConnectorMetadata(
                                           final StandardDestinationDefinition destinationDefinition,
                                           final ActorDefinitionVersion defaultVersion,
                                           final UUID scopeId,
                                           final ScopeType scopeType)
      throws IOException {
    database.transaction(ctx -> {
      writeConnectorMetadata(destinationDefinition, defaultVersion, List.of(), ctx);
      writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), scopeId,
          io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.valueOf(scopeType.toString()), ctx);
      return null;
    });

    actorDefinitionVersionUpdater.updateDestinationDefaultVersion(destinationDefinition, defaultVersion, List.of());
  }

  /**
   * Write metadata for a destination connector. Writes global metadata (destination definition) and
   * versioned metadata (info for actor definition version to set as default). Sets the new version as
   * the default version and updates actors accordingly, based on whether the upgrade will be breaking
   * or not.
   *
   * @param destinationDefinition standard destination definition
   * @param actorDefinitionVersion actor definition version
   * @param breakingChangesForDefinition - list of breaking changes for the definition
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion,
                                     final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException {
    database.transaction(ctx -> {
      writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, breakingChangesForDefinition, ctx);
      return null;
    });

    // FIXME(pedro): this should be moved out of this service
    actorDefinitionVersionUpdater.updateDestinationDefaultVersion(destinationDefinition, actorDefinitionVersion, breakingChangesForDefinition);
  }

  @Override
  public List<DestinationConnection> listDestinationsWithIds(
                                                             final List<UUID> destinationIds)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .and(ACTOR.ID.in(destinationIds))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildDestinationConnection).toList();
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

  private void writeConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                      final ActorDefinitionVersion actorDefinitionVersion,
                                      final List<ActorDefinitionBreakingChange> breakingChangesForDefinition,
                                      final DSLContext ctx) {
    writeStandardDestinationDefinition(Collections.singletonList(destinationDefinition), ctx);
    ConnectorMetadataJooqHelper.writeActorDefinitionBreakingChanges(breakingChangesForDefinition, ctx);
    ConnectorMetadataJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx);
  }

  private Stream<StandardDestinationDefinition> destDefQuery(final Optional<UUID> destDefId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.destination))
        .and(destDefId.map(ACTOR_DEFINITION.ID::eq).orElse(noCondition()))
        .and(includeTombstone ? noCondition() : ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
        .fetch())
        .stream()
        .map(DbConverter::buildStandardDestinationDefinition);
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

  private void writeDestinationConnection(final List<DestinationConnection> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((destinationConnection) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR)
          .where(ACTOR.ID.eq(destinationConnection.getDestinationId())));

      if (isExistingConfig) {
        ctx.update(ACTOR)
            .set(ACTOR.ID, destinationConnection.getDestinationId())
            .set(ACTOR.WORKSPACE_ID, destinationConnection.getWorkspaceId())
            .set(ACTOR.ACTOR_DEFINITION_ID, destinationConnection.getDestinationDefinitionId())
            .set(ACTOR.NAME, destinationConnection.getName())
            .set(ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationConnection.getConfiguration())))
            .set(ACTOR.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR.TOMBSTONE, destinationConnection.getTombstone() != null && destinationConnection.getTombstone())
            .set(ACTOR.UPDATED_AT, timestamp)
            .where(ACTOR.ID.eq(destinationConnection.getDestinationId()))
            .execute();

      } else {
        ctx.insertInto(ACTOR)
            .set(ACTOR.ID, destinationConnection.getDestinationId())
            .set(ACTOR.WORKSPACE_ID, destinationConnection.getWorkspaceId())
            .set(ACTOR.ACTOR_DEFINITION_ID, destinationConnection.getDestinationDefinitionId())
            .set(ACTOR.NAME, destinationConnection.getName())
            .set(ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationConnection.getConfiguration())))
            .set(ACTOR.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR.TOMBSTONE, destinationConnection.getTombstone() != null && destinationConnection.getTombstone())
            .set(ACTOR.CREATED_AT, timestamp)
            .set(ACTOR.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  private Condition includeTombstones(final Field<Boolean> tombstoneField, final boolean includeTombstones) {
    if (includeTombstones) {
      return DSL.trueCondition();
    } else {
      return tombstoneField.eq(false);
    }
  }

  private <T> Entry<T, Boolean> actorDefinitionWithGrantStatus(final Record outerJoinRecord,
                                                               final Function<Record, T> recordToActorDefinition) {
    final T actorDefinition = recordToActorDefinition.apply(outerJoinRecord);
    final boolean granted = outerJoinRecord.get(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID) != null;
    return Map.entry(actorDefinition, granted);
  }

  private Stream<DestinationConnection> listDestinationQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR);
      if (configId.isPresent()) {
        return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.destination), ACTOR.ID.eq(configId.get())).fetch();
      }
      return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.destination)).fetch();
    });

    return result.map(DbConverter::buildDestinationConnection).stream();
  }

  static void writeStandardDestinationDefinition(final List<StandardDestinationDefinition> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((standardDestinationDefinition) -> {
      final boolean isExistingConfig = ctx.fetchExists(DSL.select()
          .from(Tables.ACTOR_DEFINITION)
          .where(Tables.ACTOR_DEFINITION.ID.eq(standardDestinationDefinition.getDestinationDefinitionId())));

      if (isExistingConfig) {
        ctx.update(Tables.ACTOR_DEFINITION)
            .set(Tables.ACTOR_DEFINITION.ID, standardDestinationDefinition.getDestinationDefinitionId())
            .set(Tables.ACTOR_DEFINITION.NAME, standardDestinationDefinition.getName())
            .set(Tables.ACTOR_DEFINITION.ICON, standardDestinationDefinition.getIcon())
            .set(Tables.ACTOR_DEFINITION.ICON_URL, standardDestinationDefinition.getIconUrl())
            .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.destination)
            .set(Tables.ACTOR_DEFINITION.TOMBSTONE, standardDestinationDefinition.getTombstone())
            .set(Tables.ACTOR_DEFINITION.PUBLIC, standardDestinationDefinition.getPublic())
            .set(Tables.ACTOR_DEFINITION.CUSTOM, standardDestinationDefinition.getCustom())
            .set(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                standardDestinationDefinition.getResourceRequirements() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getResourceRequirements())))
            .set(ACTOR_DEFINITION.METRICS,
                standardDestinationDefinition.getMetrics() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getMetrics())))
            .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
            .where(Tables.ACTOR_DEFINITION.ID.eq(standardDestinationDefinition.getDestinationDefinitionId()))
            .execute();

      } else {
        ctx.insertInto(Tables.ACTOR_DEFINITION)
            .set(Tables.ACTOR_DEFINITION.ID, standardDestinationDefinition.getDestinationDefinitionId())
            .set(Tables.ACTOR_DEFINITION.NAME, standardDestinationDefinition.getName())
            .set(Tables.ACTOR_DEFINITION.ICON, standardDestinationDefinition.getIcon())
            .set(Tables.ACTOR_DEFINITION.ICON_URL, standardDestinationDefinition.getIconUrl())
            .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.destination)
            .set(Tables.ACTOR_DEFINITION.TOMBSTONE,
                standardDestinationDefinition.getTombstone() != null && standardDestinationDefinition.getTombstone())
            .set(Tables.ACTOR_DEFINITION.PUBLIC, standardDestinationDefinition.getPublic())
            .set(Tables.ACTOR_DEFINITION.CUSTOM, standardDestinationDefinition.getCustom())
            .set(Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                standardDestinationDefinition.getResourceRequirements() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getResourceRequirements())))
            .set(ACTOR_DEFINITION.METRICS,
                standardDestinationDefinition.getMetrics() == null ? null
                    : JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getMetrics())))
            .set(Tables.ACTOR_DEFINITION.CREATED_AT, timestamp)
            .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  /**
   * Get Destination with secrets.
   *
   * @param destinationId destination id
   * @return destination with secrets
   */
  @Override
  public DestinationConnection getDestinationConnectionWithSecrets(final UUID destinationId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final DestinationConnection destination = getDestinationConnection(destinationId);
    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(destination.getWorkspaceId());
    final JsonNode hydratedConfig;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get());
      hydratedConfig = secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(destination.getConfiguration(),
          new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      hydratedConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(destination.getConfiguration());
    }
    return Jsons.clone(destination).withConfiguration(hydratedConfig);
  }

  /**
   * Delete destination: tombstone destination AND delete secrets
   *
   * @param name Destination name
   * @param workspaceId workspace ID
   * @param destinationId destination ID
   * @param spec spec for the destination
   * @throws JsonValidationException if the config is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Override
  public void tombstoneDestination(
                                   final String name,
                                   final UUID workspaceId,
                                   final UUID destinationId,
                                   final ConnectorSpecification spec)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    // 1. Delete secrets from config
    final DestinationConnection destinationConnection = getDestinationConnection(destinationId);
    final JsonNode config = destinationConnection.getConfiguration();
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

    // 2. Tombstone destination and void config
    final DestinationConnection newDestinationConnection = new DestinationConnection()
        .withName(name)
        .withDestinationDefinitionId(destinationConnection.getDestinationDefinitionId())
        .withWorkspaceId(workspaceId)
        .withDestinationId(destinationId)
        .withConfiguration(null)
        .withTombstone(true);
    writeDestinationConnectionNoSecrets(newDestinationConnection);
  }

  /**
   * Write a destination with its secrets to the appropriate persistence. Secrets go to secrets store
   * and the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param destination to write
   * @param connectorSpecification spec for the destination
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Override
  public void writeDestinationConnectionWithSecrets(
                                                    final DestinationConnection destination,
                                                    final ConnectorSpecification connectorSpecification)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final Optional<JsonNode> previousDestinationConnection =
        getDestinationIfExists(destination.getDestinationId()).map(DestinationConnection::getConfiguration);

    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(destination.getWorkspaceId());
    RuntimeSecretPersistence secretPersistence = null;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get());
      secretPersistence = new RuntimeSecretPersistence(secretPersistenceConfig);
    }

    final JsonNode partialConfig;
    if (previousDestinationConnection.isPresent()) {
      partialConfig = secretsRepositoryWriter.updateFromConfig(destination.getWorkspaceId(),
          previousDestinationConnection.get(),
          destination.getConfiguration(),
          connectorSpecification.getConnectionSpecification(), secretPersistence);
    } else {
      partialConfig = secretsRepositoryWriter.createFromConfig(destination.getWorkspaceId(),
          destination.getConfiguration(),
          connectorSpecification.getConnectionSpecification(),
          secretPersistence);
    }

    final DestinationConnection partialSource = Jsons.clone(destination).withConfiguration(partialConfig);
    writeDestinationConnectionNoSecrets(partialSource);
  }

  private Optional<DestinationConnection> getDestinationIfExists(final UUID destinationId) {
    try {
      return Optional.of(getDestinationConnection(destinationId));
    } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
      log.warn("Unable to find destination with ID {}", destinationId);
      return Optional.empty();
    }
  }

}
