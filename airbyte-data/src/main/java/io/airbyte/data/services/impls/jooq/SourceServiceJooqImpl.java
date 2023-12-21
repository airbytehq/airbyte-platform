/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigWithMetadata;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.SourceType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionWorkspaceGrantRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
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
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.DSL;

@Slf4j
@Singleton
public class SourceServiceJooqImpl implements SourceService {

  public static final String PRIMARY_KEY = "id";

  private final ExceptionWrappingDatabase database;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final static long heartbeatMaxSecondBetweenMessage = 3600L;

  public SourceServiceJooqImpl(@Named("configDatabase") final Database database,
                               final FeatureFlagClient featureFlagClient,
                               final SecretsRepositoryReader secretsRepositoryReader,
                               final SecretsRepositoryWriter secretsRepositoryWriter,
                               final SecretPersistenceConfigService secretPersistenceConfigService) {
    this.database = new ExceptionWrappingDatabase(database);
    this.featureFlagClient = featureFlagClient;
    this.secretRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
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
      final StandardSync sync = getStandardSyncWithMetadata(connectionId).getConfig();
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
        record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessage),
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
        record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessage),
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
            entry -> DbConverter.buildStandardSourceDefinition(entry, heartbeatMaxSecondBetweenMessage)),
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
   * Delete a source by id.
   *
   * @param sourceId
   * @return true if a source was deleted, false otherwise.
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   * @throws IOException - you never know when you IO
   */
  @Override
  public boolean deleteSource(final UUID sourceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    return deleteById(ACTOR, sourceId);
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
      final StandardSourceDefinition definition = DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessage);
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
  }

  /**
   * Returns all active sources whose default_version_id is in a given list of version IDs.
   *
   * @param actorDefinitionVersionIds - list of actor definition version ids
   * @return list of SourceConnections
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<SourceConnection> listSourcesWithVersionIds(
                                                          final List<UUID> actorDefinitionVersionIds)
      throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.DEFAULT_VERSION_ID.in(actorDefinitionVersionIds))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildSourceConnection).toList();
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
    writeActorDefinitionBreakingChanges(breakingChangesForDefinition, ctx);
    ActorDefinitionVersionJooqHelper.setActorDefinitionVersionForTagAsDefault(actorDefinitionVersion, breakingChangesForDefinition, ctx);
  }

  /**
   * Writes a list of actor definition breaking changes in one transaction. Updates entries if they
   * already exist.
   *
   * @param breakingChanges - actor definition breaking changes to write
   * @param ctx database context
   * @throws IOException - you never know when you io
   */
  private void writeActorDefinitionBreakingChanges(final List<ActorDefinitionBreakingChange> breakingChanges, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final List<Query> upsertQueries = breakingChanges.stream()
        .map(breakingChange -> upsertBreakingChangeQuery(ctx, breakingChange, timestamp))
        .collect(Collectors.toList());
    ctx.batch(upsertQueries).execute();
  }

  private Query upsertBreakingChangeQuery(final DSLContext ctx, final ActorDefinitionBreakingChange breakingChange, final OffsetDateTime timestamp) {
    return ctx.insertInto(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, breakingChange.getActorDefinitionId())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION, breakingChange.getVersion().serialize())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.getUpgradeDeadline()))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.getMessage())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.getMigrationDocumentationUrl())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.getScopedImpact())))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.CREATED_AT, timestamp)
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp)
        .onConflict(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION).doUpdate()
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.getUpgradeDeadline()))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.getMessage())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.getMigrationDocumentationUrl())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.getScopedImpact())))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp);
  }

  private ConfigWithMetadata<StandardSync> getStandardSyncWithMetadata(final UUID connectionId) throws IOException, ConfigNotFoundException {
    final List<ConfigWithMetadata<StandardSync>> result = listStandardSyncWithMetadata(Optional.of(connectionId));

    final boolean foundMoreThanOneConfig = result.size() > 1;
    if (result.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC, connectionId.toString());
    } else if (foundMoreThanOneConfig) {
      throw new IllegalStateException(String.format("Multiple %s configs found for ID %s: %s", ConfigSchema.STANDARD_SYNC, connectionId, result));
    }
    return result.get(0);
  }

  private List<ConfigWithMetadata<StandardSync>> listStandardSyncWithMetadata(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(CONNECTION.asterisk(),
          SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
          .from(CONNECTION)
          // The schema management can be non-existent for a connection id, thus we need to do a left join
          .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID));
      if (configId.isPresent()) {
        return query.where(CONNECTION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    final List<ConfigWithMetadata<StandardSync>> standardSyncs = new ArrayList<>();
    for (final Record record : result) {
      final List<NotificationConfigurationRecord> notificationConfigurationRecords = database.query(ctx -> {
        if (configId.isPresent()) {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(configId.get()))
              .fetch();
        } else {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .fetch();
        }
      });

      final StandardSync standardSync =
          DbConverter.buildStandardSync(record, connectionOperationIds(record.get(CONNECTION.ID)), notificationConfigurationRecords);
      if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
        throw new RuntimeException("unexpected schedule type mismatch");
      }
      standardSyncs.add(new ConfigWithMetadata<>(
          record.get(CONNECTION.ID).toString(),
          ConfigSchema.STANDARD_SYNC.name(),
          record.get(CONNECTION.CREATED_AT).toInstant(),
          record.get(CONNECTION.UPDATED_AT).toInstant(),
          standardSync));
    }
    return standardSyncs;
  }

  private List<UUID> connectionOperationIds(final UUID connectionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(CONNECTION_OPERATION)
        .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
        .fetch());

    final List<UUID> ids = new ArrayList<>();
    for (final Record record : result) {
      ids.add(record.get(CONNECTION_OPERATION.OPERATION_ID));
    }

    return ids;
  }

  private Stream<StandardSourceDefinition> sourceDefQuery(final Optional<UUID> sourceDefId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.source))
        .and(sourceDefId.map(ACTOR_DEFINITION.ID::eq).orElse(noCondition()))
        .and(includeTombstone ? noCondition() : ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
        .fetch())
        .stream()
        .map(record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessage));
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

  public Optional<UUID> getOrganizationIdFromWorkspaceId(final UUID scopeId) throws IOException {
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

  static void writeStandardSourceDefinition(final List<StandardSourceDefinition> configs, final DSLContext ctx) {
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
            .where(Tables.ACTOR_DEFINITION.ID.eq(standardSourceDefinition.getSourceDefinitionId()))
            .execute();

      } else {
        ctx.insertInto(Tables.ACTOR_DEFINITION)
            .set(Tables.ACTOR_DEFINITION.ID, standardSourceDefinition.getSourceDefinitionId())
            .set(Tables.ACTOR_DEFINITION.NAME, standardSourceDefinition.getName())
            .set(Tables.ACTOR_DEFINITION.ICON, standardSourceDefinition.getIcon())
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
        final UUID actorDefinitionDefaultVersionId =
            getDefaultVersionForActorDefinitionId(sourceConnection.getSourceDefinitionId(), ctx).getVersionId();
        ctx.insertInto(ACTOR)
            .set(ACTOR.ID, sourceConnection.getSourceId())
            .set(ACTOR.WORKSPACE_ID, sourceConnection.getWorkspaceId())
            .set(ACTOR.ACTOR_DEFINITION_ID, sourceConnection.getSourceDefinitionId())
            .set(ACTOR.NAME, sourceConnection.getName())
            .set(ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceConnection.getConfiguration())))
            .set(ACTOR.ACTOR_TYPE, ActorType.source)
            .set(ACTOR.TOMBSTONE, sourceConnection.getTombstone() != null && sourceConnection.getTombstone())
            .set(ACTOR.DEFAULT_VERSION_ID, actorDefinitionDefaultVersionId)
            .set(ACTOR.CREATED_AT, timestamp)
            .set(ACTOR.UPDATED_AT, timestamp)
            .execute();
      }
    });
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
    return ActorDefinitionVersionJooqHelper.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId, ctx);
  }

  /**
   * Deletes all records with given id. If it deletes anything, returns true. Otherwise, false.
   *
   * @param table - table from which to delete the record
   * @param id - id of the record to delete
   * @return true if anything was deleted, otherwise false.
   * @throws IOException - you never know when you io
   */
  @SuppressWarnings("SameParameterValue")
  private boolean deleteById(final Table<?> table, final UUID id) throws IOException {
    return database.transaction(ctx -> ctx.deleteFrom(table)).where(field(DSL.name(PRIMARY_KEY)).eq(id)).execute() > 0;
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
          secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get());
      hydratedConfig = secretRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(source.getConfiguration(),
          new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      hydratedConfig = secretRepositoryReader.hydrateConfigFromDefaultSecretPersistence(source.getConfiguration());
    }
    return Jsons.clone(source).withConfiguration(hydratedConfig);
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

    // strip secrets
    final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(source.getWorkspaceId());
    final JsonNode partialConfig;
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get());
      partialConfig = secretsRepositoryWriter.statefulUpdateSecretsToRuntimeSecretPersistence(
          source.getWorkspaceId(),
          previousSourceConnection,
          source.getConfiguration(),
          connectorSpecification.getConnectionSpecification(),
          validate(source),
          new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      partialConfig = secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(
          source.getWorkspaceId(),
          previousSourceConnection,
          source.getConfiguration(),
          connectorSpecification.getConnectionSpecification(),
          validate(source));
    }
    final SourceConnection partialSource = Jsons.clone(source).withConfiguration(partialConfig);
    writeSourceConnectionNoSecrets(partialSource);
  }

  public Optional<SourceConnection> getSourceIfExists(final UUID sourceId) {
    try {
      return Optional.of(getSourceConnection(sourceId));
    } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
      log.warn("Unable to find source with ID {}", sourceId);
      return Optional.empty();
    }
  }

  private boolean validate(final SourceConnection source) {
    return source.getTombstone() == null || !source.getTombstone();
  }

}
