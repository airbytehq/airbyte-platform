/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTIVE_DECLARATIVE_MANIFEST;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG_FETCH_EVENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_CONFIG_INJECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_OAUTH_PARAMETER;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTOR_BUILDER_PROJECT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.DECLARATIVE_MANIFEST;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE_SERVICE_ACCOUNT;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.groupConcat;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.SQLDataType.VARCHAR;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActiveDeclarativeManifest;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.Geography;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.OperatorNormalization;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WorkspaceServiceAccount;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricQueries;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository of all SQL queries for the Configs Db. We are moving to persistences scoped by
 * resource.
 */
@SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.CyclomaticComplexity", "PMD.AvoidLiteralsInIfCondition",
  "OptionalUsedAsFieldOrParameterType"})
public class ConfigRepository {

  /**
   * Query object for querying connections for a workspace.
   *
   * @param workspaceId workspace to fetch connections for
   * @param sourceId fetch connections with this source id
   * @param destinationId fetch connections with this destination id
   * @param includeDeleted include tombstoned connections
   */
  public record StandardSyncQuery(@Nonnull UUID workspaceId, List<UUID> sourceId, List<UUID> destinationId, boolean includeDeleted) {

  }

  /**
   * Query object for paginated querying of connections in multiple workspaces.
   *
   * @param workspaceIds workspaces to fetch connections for
   * @param sourceId fetch connections with this source id
   * @param destinationId fetch connections with this destination id
   * @param includeDeleted include tombstoned connections
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record StandardSyncsQueryPaginated(
                                            @Nonnull List<UUID> workspaceIds,
                                            List<UUID> sourceId,
                                            List<UUID> destinationId,
                                            boolean includeDeleted,
                                            int pageSize,
                                            int rowOffset) {}

  /**
   * Query object for paginated querying of sources/destinations in multiple workspaces.
   *
   * @param workspaceIds workspaces to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record ResourcesQueryPaginated(
                                        @Nonnull List<UUID> workspaceIds,
                                        boolean includeDeleted,
                                        int pageSize,
                                        int rowOffset) {}

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRepository.class);
  private static final String OPERATION_IDS_AGG_FIELD = "operation_ids_agg";
  private static final String OPERATION_IDS_AGG_DELIMITER = ",";
  public static final String PRIMARY_KEY = "id";
  private static final List<Field<?>> BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS =
      Arrays.asList(CONNECTOR_BUILDER_PROJECT.ID, CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, CONNECTOR_BUILDER_PROJECT.NAME,
          CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID, CONNECTOR_BUILDER_PROJECT.TOMBSTONE,
          field(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull()).as("hasDraft"));
  private static final UUID VOID_UUID = new UUID(0, 0);

  private final ExceptionWrappingDatabase database;
  private final ActorDefinitionMigrator actorDefinitionMigrator;
  private final StandardSyncPersistence standardSyncPersistence;

  private final Supplier<Long> heartbeatMaxSecondBetweenMessageSupplier;

  public ConfigRepository(final Database database, final Supplier<Long> heartbeatMaxSecondBetweenMessageSupplier) {
    this(database, new ActorDefinitionMigrator(new ExceptionWrappingDatabase(database)), new StandardSyncPersistence(database),
        heartbeatMaxSecondBetweenMessageSupplier);
  }

  ConfigRepository(final Database database,
                   final ActorDefinitionMigrator actorDefinitionMigrator,
                   final StandardSyncPersistence standardSyncPersistence,
                   final Supplier<Long> heartbeatMaxSecondBetweenMessageSupplier) {
    this.database = new ExceptionWrappingDatabase(database);
    this.actorDefinitionMigrator = actorDefinitionMigrator;
    this.standardSyncPersistence = standardSyncPersistence;
    this.heartbeatMaxSecondBetweenMessageSupplier = heartbeatMaxSecondBetweenMessageSupplier;
  }

  /**
   * Conduct a health check by attempting to read from the database. This query needs to be fast as
   * this call can be made multiple times a second.
   *
   * @return true if read succeeds, even if the table is empty, and false if any error happens.
   */
  public boolean healthCheck() {
    try {
      // The only supported database is Postgres, so we can call SELECT 1 to test connectivity.
      database.query(ctx -> ctx.fetch("SELECT 1")).stream().count();
    } catch (final Exception e) {
      LOGGER.error("Health check error: ", e);
      return false;
    }
    return true;
  }

  /**
   * Get workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include tombestoned workspace
   * @return workspace
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  public StandardWorkspace getStandardWorkspaceNoSecrets(final UUID workspaceId, final boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listWorkspaceQuery(Optional.of(workspaceId), includeTombstone)
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, workspaceId));
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombestoned workspace
   * @return workspace, if present.
   * @throws IOException - you never know when you IO
   */
  public Optional<StandardWorkspace> getWorkspaceBySlugOptional(final String slug, final boolean includeTombstone)
      throws IOException {
    final Result<Record> result;
    if (includeTombstone) {
      result = database.query(ctx -> ctx.select(WORKSPACE.asterisk())
          .from(WORKSPACE)
          .where(WORKSPACE.SLUG.eq(slug))).fetch();
    } else {
      result = database.query(ctx -> ctx.select(WORKSPACE.asterisk())
          .from(WORKSPACE)
          .where(WORKSPACE.SLUG.eq(slug)).andNot(WORKSPACE.TOMBSTONE)).fetch();
    }

    return result.stream().findFirst().map(DbConverter::buildStandardWorkspace);
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombestoned workspace
   * @return workspace
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  public StandardWorkspace getWorkspaceBySlug(final String slug, final boolean includeTombstone) throws IOException, ConfigNotFoundException {
    return getWorkspaceBySlugOptional(slug, includeTombstone).orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, slug));
  }

  /**
   * List workspaces.
   *
   * @param includeTombstone include tombstoned workspaces
   * @return workspaces
   * @throws IOException - you never know when you IO
   */
  public List<StandardWorkspace> listStandardWorkspaces(final boolean includeTombstone) throws IOException {
    return listWorkspaceQuery(Optional.empty(), includeTombstone).toList();
  }

  private Stream<StandardWorkspace> listWorkspaceQuery(final Optional<UUID> workspaceId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(includeTombstone ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .and(workspaceId.map(WORKSPACE.ID::eq).orElse(noCondition()))
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace);
  }

  /**
   * List workspaces (paginated).
   *
   * @param resourcesQueryPaginated - contains all the information we need to paginate
   * @return A List of StandardWorkspace objectjs
   * @throws IOException you never know when you IO
   */
  public List<StandardWorkspace> listStandardWorkspacesPaginated(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(resourcesQueryPaginated.includeDeleted() ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .and(WORKSPACE.ID.in(resourcesQueryPaginated.workspaceIds()))
        .limit(resourcesQueryPaginated.pageSize())
        .offset(resourcesQueryPaginated.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }.
   *
   * Write a StandardWorkspace to the database.
   *
   * @param workspace - The configuration of the workspace
   * @throws JsonValidationException - throws is the workspace is invalid
   * @throws IOException - you never know when you IO
   */
  public void writeStandardWorkspaceNoSecrets(final StandardWorkspace workspace) throws JsonValidationException, IOException {
    database.transaction(ctx -> {
      final OffsetDateTime timestamp = OffsetDateTime.now();
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(WORKSPACE)
          .where(WORKSPACE.ID.eq(workspace.getWorkspaceId())));

      if (isExistingConfig) {
        ctx.update(WORKSPACE)
            .set(WORKSPACE.ID, workspace.getWorkspaceId())
            .set(WORKSPACE.CUSTOMER_ID, workspace.getCustomerId())
            .set(WORKSPACE.NAME, workspace.getName())
            .set(WORKSPACE.SLUG, workspace.getSlug())
            .set(WORKSPACE.EMAIL, workspace.getEmail())
            .set(WORKSPACE.INITIAL_SETUP_COMPLETE, workspace.getInitialSetupComplete())
            .set(WORKSPACE.ANONYMOUS_DATA_COLLECTION, workspace.getAnonymousDataCollection())
            .set(WORKSPACE.SEND_NEWSLETTER, workspace.getNews())
            .set(WORKSPACE.SEND_SECURITY_UPDATES, workspace.getSecurityUpdates())
            .set(WORKSPACE.DISPLAY_SETUP_WIZARD, workspace.getDisplaySetupWizard())
            .set(WORKSPACE.TOMBSTONE, workspace.getTombstone() != null && workspace.getTombstone())
            .set(WORKSPACE.NOTIFICATIONS, JSONB.valueOf(Jsons.serialize(workspace.getNotifications())))
            .set(WORKSPACE.FIRST_SYNC_COMPLETE, workspace.getFirstCompletedSync())
            .set(WORKSPACE.FEEDBACK_COMPLETE, workspace.getFeedbackDone())
            .set(WORKSPACE.GEOGRAPHY, Enums.toEnum(
                workspace.getDefaultGeography().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
            .set(WORKSPACE.UPDATED_AT, timestamp)
            .set(WORKSPACE.WEBHOOK_OPERATION_CONFIGS, workspace.getWebhookOperationConfigs() == null ? null
                : JSONB.valueOf(Jsons.serialize(workspace.getWebhookOperationConfigs())))
            .where(WORKSPACE.ID.eq(workspace.getWorkspaceId()))
            .execute();
      } else {
        ctx.insertInto(WORKSPACE)
            .set(WORKSPACE.ID, workspace.getWorkspaceId())
            .set(WORKSPACE.CUSTOMER_ID, workspace.getCustomerId())
            .set(WORKSPACE.NAME, workspace.getName())
            .set(WORKSPACE.SLUG, workspace.getSlug())
            .set(WORKSPACE.EMAIL, workspace.getEmail())
            .set(WORKSPACE.INITIAL_SETUP_COMPLETE, workspace.getInitialSetupComplete())
            .set(WORKSPACE.ANONYMOUS_DATA_COLLECTION, workspace.getAnonymousDataCollection())
            .set(WORKSPACE.SEND_NEWSLETTER, workspace.getNews())
            .set(WORKSPACE.SEND_SECURITY_UPDATES, workspace.getSecurityUpdates())
            .set(WORKSPACE.DISPLAY_SETUP_WIZARD, workspace.getDisplaySetupWizard())
            .set(WORKSPACE.TOMBSTONE, workspace.getTombstone() != null && workspace.getTombstone())
            .set(WORKSPACE.NOTIFICATIONS, JSONB.valueOf(Jsons.serialize(workspace.getNotifications())))
            .set(WORKSPACE.FIRST_SYNC_COMPLETE, workspace.getFirstCompletedSync())
            .set(WORKSPACE.FEEDBACK_COMPLETE, workspace.getFeedbackDone())
            .set(WORKSPACE.CREATED_AT, timestamp)
            .set(WORKSPACE.UPDATED_AT, timestamp)
            .set(WORKSPACE.GEOGRAPHY, Enums.toEnum(
                workspace.getDefaultGeography().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
            .set(WORKSPACE.WEBHOOK_OPERATION_CONFIGS, workspace.getWebhookOperationConfigs() == null ? null
                : JSONB.valueOf(Jsons.serialize(workspace.getWebhookOperationConfigs())))
            .execute();
      }
      return null;

    });
  }

  /**
   * Set user feedback on workspace.
   *
   * @param workspaceId workspace id.
   * @throws IOException - you never know when you IO
   */
  public void setFeedback(final UUID workspaceId) throws IOException {
    database.query(ctx -> ctx.update(WORKSPACE).set(WORKSPACE.FEEDBACK_COMPLETE, true).where(WORKSPACE.ID.eq(workspaceId)).execute());
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
  public StandardSourceDefinition getSourceDefinitionFromConnection(final UUID connectionId) {
    try {
      final StandardSync sync = getStandardSync(connectionId);
      return getSourceDefinitionFromSource(sync.getSourceId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get workspace for a connection.
   *
   * @param connectionId connection id
   * @param isTombstone include tombstoned workspaces
   * @return workspace to which the connection belongs
   */
  public StandardWorkspace getStandardWorkspaceFromConnection(final UUID connectionId, final boolean isTombstone) {
    try {
      final StandardSync sync = getStandardSync(connectionId);
      final SourceConnection source = getSourceConnection(sync.getSourceId());
      return getStandardWorkspaceNoSecrets(source.getWorkspaceId(), isTombstone);
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
  public List<StandardSourceDefinition> listStandardSourceDefinitions(final boolean includeTombstone) throws IOException {
    return sourceDefQuery(Optional.empty(), includeTombstone).toList();
  }

  private Stream<StandardSourceDefinition> sourceDefQuery(final Optional<UUID> sourceDefId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.source))
        .and(sourceDefId.map(ACTOR_DEFINITION.ID::eq).orElse(noCondition()))
        .and(includeTombstone ? noCondition() : ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
        .fetch())
        .stream()
        .map(record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessageSupplier.get()))
        // Ensure version is set. Needed for connectors not upgraded since we added versioning.
        .map(def -> def.withProtocolVersion(AirbyteProtocolVersion.getWithDefault(def.getProtocolVersion()).serialize()));
  }

  /**
   * Get actor definition ids to pair of actor type and protocol version.
   *
   * @return map of definition id to pair of actor type and protocol version.
   * @throws IOException - you never know when you IO
   */
  public Map<UUID, Map.Entry<io.airbyte.config.ActorType, Version>> getActorDefinitionToProtocolVersionMap() throws IOException {
    return database.query(ConfigWriter::getActorDefinitionsInUseToProtocolVersion);
  }

  /**
   * List public source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return public source definitions
   * @throws IOException - you never know when you IO
   */
  public List<StandardSourceDefinition> listPublicSourceDefinitions(final boolean includeTombstone) throws IOException {
    return listStandardActorDefinitions(
        ActorType.source,
        record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessageSupplier.get()),
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
  public List<StandardSourceDefinition> listGrantedSourceDefinitions(final UUID workspaceId, final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.JOIN,
        ActorType.source,
        record -> DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessageSupplier.get()),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * List source to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombestoned definitions
   * @return list of pairs from source definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  public List<Entry<StandardSourceDefinition, Boolean>> listGrantableSourceDefinitions(final UUID workspaceId,
                                                                                       final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.LEFT_OUTER_JOIN,
        ActorType.source,
        record -> actorDefinitionWithGrantStatus(record,
            entry -> DbConverter.buildStandardSourceDefinition(entry, heartbeatMaxSecondBetweenMessageSupplier.get())),
        ACTOR_DEFINITION.CUSTOM.eq(false),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * Write source definition.
   *
   * @param sourceDefinition source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   */
  public void writeStandardSourceDefinition(final StandardSourceDefinition sourceDefinition) throws JsonValidationException, IOException {
    database.transaction(ctx -> {
      ConfigWriter.writeStandardSourceDefinition(Collections.singletonList(sourceDefinition), ctx);
      return null;
    });
  }

  /**
   * Update the docker image tag for multiple actor definitions at once.
   *
   * @param actorDefinitionIds the list of actor definition ids to update
   * @param targetImageTag the new docker image tag for these actor definitions
   * @throws IOException - you never know when you IO
   */
  public int updateActorDefinitionsDockerImageTag(final List<UUID> actorDefinitionIds, final String targetImageTag) throws IOException {
    return database.transaction(ctx -> ConfigWriter.writeSourceDefinitionImageTag(actorDefinitionIds, targetImageTag, ctx));
  }

  /**
   * Wirte custom source definition.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @throws IOException - you never know when you IO
   */
  public void writeCustomSourceDefinition(final StandardSourceDefinition sourceDefinition, final UUID workspaceId)
      throws IOException {
    database.transaction(ctx -> {
      ConfigWriter.writeStandardSourceDefinition(Collections.singletonList(sourceDefinition), ctx);
      writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), workspaceId, ctx);
      return null;
    });
  }

  private void updateDeclarativeActorDefinition(final ActorDefinitionConfigInjection configInjection,
                                                final ConnectorSpecification spec,
                                                final DSLContext ctx) {
    ctx.update(Tables.ACTOR_DEFINITION)
        .set(Tables.ACTOR_DEFINITION.SPEC, JSONB.valueOf(Jsons.serialize(spec)))
        .where(Tables.ACTOR_DEFINITION.ID.eq(configInjection.getActorDefinitionId()))
        .execute();
    writeActorDefinitionConfigInjectionForPath(configInjection, ctx);
  }

  private Stream<StandardDestinationDefinition> destDefQuery(final Optional<UUID> destDefId, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION.asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.destination))
        .and(destDefId.map(ACTOR_DEFINITION.ID::eq).orElse(noCondition()))
        .and(includeTombstone ? noCondition() : ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
        .fetch())
        .stream()
        .map(DbConverter::buildStandardDestinationDefinition)
        // Ensure version is set. Needed for connectors not upgraded since we added versioning.
        .map(def -> def.withProtocolVersion(AirbyteProtocolVersion.getWithDefault(def.getProtocolVersion()).serialize()));
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
  public StandardDestinationDefinition getStandardDestinationDefinition(final UUID destinationDefinitionId)
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
  public StandardDestinationDefinition getDestinationDefinitionFromDestination(final UUID destinationId) {
    try {
      final DestinationConnection destination = getDestinationConnection(destinationId);
      return getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get destination definition used by a connection.
   *
   * @param connectionId connection id
   * @return destination definition
   */
  public StandardDestinationDefinition getDestinationDefinitionFromConnection(final UUID connectionId) {
    try {
      final StandardSync sync = getStandardSync(connectionId);
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
  public List<StandardDestinationDefinition> listStandardDestinationDefinitions(final boolean includeTombstone) throws IOException {
    return destDefQuery(Optional.empty(), includeTombstone).toList();
  }

  /**
   * List public destination definitions.
   *
   * @param includeTombstone include tombstoned destinations
   * @return public destination definitions
   * @throws IOException - you never know when you IO
   */
  public List<StandardDestinationDefinition> listPublicDestinationDefinitions(final boolean includeTombstone) throws IOException {
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
  public List<StandardDestinationDefinition> listGrantedDestinationDefinitions(final UUID workspaceId, final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.JOIN,
        ActorType.destination,
        DbConverter::buildStandardDestinationDefinition,
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * List destinations to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombestoned definitions
   * @return list of pairs from destination definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  public List<Entry<StandardDestinationDefinition, Boolean>> listGrantableDestinationDefinitions(final UUID workspaceId,
                                                                                                 final boolean includeTombstones)
      throws IOException {
    return listActorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.LEFT_OUTER_JOIN,
        ActorType.destination,
        record -> actorDefinitionWithGrantStatus(record, DbConverter::buildStandardDestinationDefinition),
        ACTOR_DEFINITION.CUSTOM.eq(false),
        includeTombstones(ACTOR_DEFINITION.TOMBSTONE, includeTombstones));
  }

  /**
   * Write destination definition.
   *
   * @param destinationDefinition destination definition
   * @throws IOException - you never know when you IO
   */
  public void writeStandardDestinationDefinition(final StandardDestinationDefinition destinationDefinition)
      throws JsonValidationException, IOException {
    database.transaction(ctx -> {
      ConfigWriter.writeStandardDestinationDefinition(Collections.singletonList(destinationDefinition), ctx);
      return null;
    });
  }

  /**
   * Write custom destination definition.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @throws IOException - you never know when you IO
   */
  public void writeCustomDestinationDefinition(final StandardDestinationDefinition destinationDefinition, final UUID workspaceId)
      throws IOException {
    database.transaction(ctx -> {
      ConfigWriter.writeStandardDestinationDefinition(List.of(destinationDefinition), ctx);
      writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), workspaceId, ctx);
      return null;
    });
  }

  /**
   * Delete connection.
   *
   * @param syncId connection id
   * @throws IOException - you never know when you IO
   */
  public void deleteStandardSync(final UUID syncId) throws IOException {
    standardSyncPersistence.deleteStandardSync(syncId);
  }

  /**
   * Write actor definition workspace grant.
   *
   * @param actorDefinitionId actor definition id
   * @param workspaceId workspace id
   * @throws IOException - you never know when you IO
   */
  public void writeActorDefinitionWorkspaceGrant(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    database.query(ctx -> writeActorDefinitionWorkspaceGrant(actorDefinitionId, workspaceId, ctx));
  }

  private int writeActorDefinitionWorkspaceGrant(final UUID actorDefinitionId, final UUID workspaceId, final DSLContext ctx) {
    return ctx.insertInto(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .set(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID, actorDefinitionId)
        .set(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID, workspaceId)
        .execute();
  }

  /**
   * Test if grant exists for actor definition and workspace.
   *
   * @param actorDefinitionId actor definition id
   * @param workspaceId workspace id
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  public boolean actorDefinitionWorkspaceGrantExists(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    final Integer count = database.query(ctx -> ctx.fetchCount(
        DSL.selectFrom(ACTOR_DEFINITION_WORKSPACE_GRANT)
            .where(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .and(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID.eq(workspaceId))));
    return count == 1;
  }

  /**
   * Delete workspace access to actor definition.
   *
   * @param actorDefinitionId actor definition id to remove
   * @param workspaceId workspace to remove actor definition from
   * @throws IOException - you never know when you IO
   */
  public void deleteActorDefinitionWorkspaceGrant(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    database.query(ctx -> ctx.deleteFrom(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .where(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .and(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID.eq(workspaceId))
        .execute());
  }

  /**
   * Test if workspace is has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param workspaceId workspace id
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  public boolean workspaceCanUseDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.LEFT_OUTER_JOIN,
        ACTOR_DEFINITION.ID.eq(actorDefinitionId),
        ACTOR_DEFINITION.PUBLIC.eq(true).or(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID.eq(workspaceId)));
    return records.isNotEmpty();
  }

  /**
   * Test if workspace is has access to a custom connector definition.
   *
   * @param actorDefinitionId custom actor definition id
   * @param workspaceId workspace id
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  public boolean workspaceCanUseCustomDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        workspaceId,
        JoinType.JOIN,
        ACTOR_DEFINITION.ID.eq(actorDefinitionId),
        ACTOR_DEFINITION.CUSTOM.eq(true));
    return records.isNotEmpty();
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

  private <T> List<T> listActorDefinitionsJoinedWithGrants(final UUID workspaceId,
                                                           final JoinType joinType,
                                                           final ActorType actorType,
                                                           final Function<Record, T> recordToReturnType,
                                                           final Condition... conditions)
      throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        workspaceId,
        joinType,
        ArrayUtils.addAll(conditions,
            ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType),
            ACTOR_DEFINITION.PUBLIC.eq(false)));

    return records.stream()
        .map(recordToReturnType)
        .toList();
  }

  private <T> Entry<T, Boolean> actorDefinitionWithGrantStatus(final Record outerJoinRecord,
                                                               final Function<Record, T> recordToActorDefinition) {
    final T actorDefinition = recordToActorDefinition.apply(outerJoinRecord);
    final boolean granted = outerJoinRecord.get(ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID) != null;
    return Map.entry(actorDefinition, granted);
  }

  private Result<Record> actorDefinitionsJoinedWithGrants(final UUID workspaceId,
                                                          final JoinType joinType,
                                                          final Condition... conditions)
      throws IOException {
    return database.query(ctx -> ctx.select(asterisk()).from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_WORKSPACE_GRANT, joinType)
        .on(ACTOR_DEFINITION.ID.eq(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID),
            ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID.eq(workspaceId))
        .where(conditions)
        .fetch());
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
  public SourceConnection getSourceConnection(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    return listSourceQuery(Optional.of(sourceId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.SOURCE_CONNECTION, sourceId));
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }
   *
   * Write a SourceConnection to the database. The configuration of the Source will be a partial
   * configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialSource - The configuration of the Source will be a partial configuration (no
   *        secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  public void writeSourceConnectionNoSecrets(final SourceConnection partialSource) throws IOException {
    database.transaction(ctx -> {
      writeSourceConnection(Collections.singletonList(partialSource), ctx);
      return null;
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

  public boolean deleteSource(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    return deleteById(ACTOR, sourceId);
  }

  /**
   * Returns all sources in the database. Does not contain secrets. To hydrate with secrets see
   * { @link SecretsRepositoryReader#listSourceConnectionWithSecrets() }.
   *
   * @return sources
   * @throws IOException - you never know when you IO
   */
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
  public List<SourceConnection> listWorkspacesSourceConnections(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
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

  /**
   * Returns destination with a given id. Does not contain secrets. To hydrate with secrets see
   * { @link SecretsRepositoryReader#getDestinationConnectionWithSecrets(final UUID destinationId) }.
   *
   * @param destinationId - id of destination to fetch.
   * @return destinations
   * @throws JsonValidationException - throws if returned destinations are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no destination with that id can be found.
   */
  public DestinationConnection getDestinationConnection(final UUID destinationId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listDestinationQuery(Optional.of(destinationId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DESTINATION_CONNECTION, destinationId));
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }
   *
   * Write a DestinationConnection to the database. The configuration of the Destination will be a
   * partial configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialDestination - The configuration of the Destination will be a partial configuration
   *        (no secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  public void writeDestinationConnectionNoSecrets(final DestinationConnection partialDestination) throws IOException {
    database.transaction(ctx -> {
      writeDestinationConnection(Collections.singletonList(partialDestination), ctx);
      return null;
    });
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

  /**
   * Returns all destinations in the database. Does not contain secrets. To hydrate with secrets see
   * { @link SecretsRepositoryReader#listDestinationConnectionWithSecrets() }.
   *
   * @return destinations
   * @throws IOException - you never know when you IO
   */
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
  public List<DestinationConnection> listWorkspaceDestinationConnection(final UUID workspaceId) throws IOException {
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
  public List<DestinationConnection> listWorkspacesDestinationConnections(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
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
   * List workspace IDs with most recently running jobs within a given time window (in hours).
   *
   * @param timeWindowInHours - integer, e.g. 24, 48, etc
   * @return list of workspace IDs
   * @throws IOException - failed to query data
   */
  public List<UUID> listWorkspacesByMostRecentlyRunningJobs(final int timeWindowInHours) throws IOException {
    final Result<Record1<UUID>> records = database.query(ctx -> ctx.selectDistinct(ACTOR.WORKSPACE_ID)
        .from(ACTOR)
        .join(CONNECTION)
        .on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .join(JOBS)
        .on(CONNECTION.ID.cast(VARCHAR(255)).eq(JOBS.SCOPE))
        .where(JOBS.UPDATED_AT.greaterOrEqual(OffsetDateTime.now().minusHours(timeWindowInHours)))
        .fetch());
    return records.stream().map(record -> record.get(ACTOR.WORKSPACE_ID)).collect(Collectors.toList());
  }

  /**
   * Returns all active sources using a definition.
   *
   * @param definitionId - id for the definition
   * @return sources
   * @throws IOException - exception while interacting with the db
   */
  public List<SourceConnection> listSourcesForDefinition(final UUID definitionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .and(ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildSourceConnection).collect(Collectors.toList());
  }

  /**
   * Returns all active destinations using a definition.
   *
   * @param definitionId - id for the definition
   * @return destinations
   * @throws IOException - exception while interacting with the db
   */
  public List<DestinationConnection> listDestinationsForDefinition(final UUID definitionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(ACTOR)
        .where(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .and(ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
        .andNot(ACTOR.TOMBSTONE).fetch());
    return result.stream().map(DbConverter::buildDestinationConnection).collect(Collectors.toList());
  }

  /**
   * Get connection.
   *
   * @param connectionId connection id
   * @return connection
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public StandardSync getStandardSync(final UUID connectionId) throws JsonValidationException, IOException, ConfigNotFoundException {
    return standardSyncPersistence.getStandardSync(connectionId);
  }

  /**
   * Write connection.
   *
   * @param standardSync connection
   * @throws IOException - exception while interacting with the db
   */
  public void writeStandardSync(final StandardSync standardSync) throws IOException {
    standardSyncPersistence.writeStandardSync(standardSync);
  }

  /**
   * For the StandardSyncs related to actorDefinitionId, clear the unsupported protocol version flag
   * if both connectors are now within support range.
   *
   * @param actorDefinitionId the actorDefinitionId to query
   * @param actorType the ActorType of actorDefinitionId
   * @param supportedRange the supported range of protocol versions
   */
  // We have conflicting imports here, ActorType is imported from jooq for most internal uses. Since
  // this is a public method, we should be using the ActorType from airbyte-config.
  public void clearUnsupportedProtocolVersionFlag(final UUID actorDefinitionId,
                                                  final io.airbyte.config.ActorType actorType,
                                                  final AirbyteProtocolVersionRange supportedRange)
      throws IOException {
    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(actorDefinitionId, actorType, supportedRange);
  }

  /**
   * List connections.
   *
   * @return connections
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSync> listStandardSyncs() throws IOException {
    return standardSyncPersistence.listStandardSync();
  }

  /**
   * List connections using operation.
   *
   * @param operationId operation id.
   * @return Connections that use the operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSync> listStandardSyncsUsingOperation(final UUID operationId) throws IOException {

    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD))
        .from(CONNECTION)

        // inner join with all connection_operation rows that match the connection's id
        .join(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))

        // only keep rows for connections that have an operationId that matches the input.
        // needs to be a sub query because we want to keep all operationIds for matching connections
        // in the main query
        .where(CONNECTION.ID.in(
            select(CONNECTION.ID).from(CONNECTION).join(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
                .where(CONNECTION_OPERATION.OPERATION_ID.eq(operationId))))

        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * List connections for workspace.
   *
   * @param workspaceId workspace id
   * @param includeDeleted include deleted
   * @return list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSync> listWorkspaceStandardSyncs(final UUID workspaceId, final boolean includeDeleted) throws IOException {
    return listWorkspaceStandardSyncs(new StandardSyncQuery(workspaceId, null, null, includeDeleted));
  }

  /**
   * List connections for workspace via a query.
   *
   * @param standardSyncQuery query
   * @return list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSync> listWorkspaceStandardSyncs(final StandardSyncQuery standardSyncQuery) throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD))
        .from(CONNECTION)

        // left join with all connection_operation rows that match the connection's id.
        // left join includes connections that don't have any connection_operations
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))

        // join with source actors so that we can filter by workspaceId
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.eq(standardSyncQuery.workspaceId)
            .and(standardSyncQuery.destinationId == null || standardSyncQuery.destinationId.isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncQuery.destinationId))
            .and(standardSyncQuery.sourceId == null || standardSyncQuery.sourceId.isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncQuery.sourceId))
            .and(standardSyncQuery.includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))

        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * List connections. Paginated.
   */
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(
                                                                           final List<UUID> workspaceIds,
                                                                           final boolean includeDeleted,
                                                                           final int pageSize,
                                                                           final int rowOffset)
      throws IOException {
    return listWorkspaceStandardSyncsPaginated(new StandardSyncsQueryPaginated(
        workspaceIds,
        null,
        null,
        includeDeleted,
        pageSize,
        rowOffset));
  }

  /**
   * List connections for workspace. Paginated.
   *
   * @param standardSyncsQueryPaginated query
   * @return Map of workspace ID -> list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(final StandardSyncsQueryPaginated standardSyncsQueryPaginated)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            ACTOR.WORKSPACE_ID)
        .from(CONNECTION)

        // left join with all connection_operation rows that match the connection's id.
        // left join includes connections that don't have any connection_operations
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))

        // join with source actors so that we can filter by workspaceId
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.in(standardSyncsQueryPaginated.workspaceIds())
            .and(standardSyncsQueryPaginated.destinationId == null || standardSyncsQueryPaginated.destinationId.isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncsQueryPaginated.destinationId))
            .and(standardSyncsQueryPaginated.sourceId == null || standardSyncsQueryPaginated.sourceId.isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncsQueryPaginated.sourceId))
            .and(standardSyncsQueryPaginated.includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID, ACTOR.WORKSPACE_ID))
        .limit(standardSyncsQueryPaginated.pageSize())
        .offset(standardSyncsQueryPaginated.rowOffset())
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));
    return getWorkspaceIdToStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * List connections that use a source.
   *
   * @param sourceId source id
   * @param includeDeleted include deleted
   * @return connections that use the provided source
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSync> listConnectionsBySource(final UUID sourceId, final boolean includeDeleted) throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD))
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.SOURCE_ID.eq(sourceId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .groupBy(CONNECTION.ID)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  private List<NotificationConfigurationRecord> getNotificationConfigurationByConnectionIds(final List<UUID> connnectionIds) throws IOException {
    return database.query(ctx -> ctx.selectFrom(NOTIFICATION_CONFIGURATION)
        .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.in(connnectionIds))
        .fetch());
  }

  private List<StandardSync> getStandardSyncsFromResult(final Result<Record> connectionAndOperationIdsResult,
                                                        final List<NotificationConfigurationRecord> allNeededNotificationConfigurations) {
    final List<StandardSync> standardSyncs = new ArrayList<>();

    for (final Record record : connectionAndOperationIdsResult) {
      final String operationIdsFromRecord = record.get(OPERATION_IDS_AGG_FIELD, String.class);

      // can be null when connection has no connectionOperations
      final List<UUID> operationIds = operationIdsFromRecord == null
          ? Collections.emptyList()
          : Arrays.stream(operationIdsFromRecord.split(OPERATION_IDS_AGG_DELIMITER)).map(UUID::fromString).toList();

      final UUID connectionId = record.get(CONNECTION.ID);
      final List<NotificationConfigurationRecord> notificationConfigurationsForConnection = allNeededNotificationConfigurations.stream()
          .filter(notificationConfiguration -> notificationConfiguration.getConnectionId().equals(connectionId))
          .toList();
      standardSyncs.add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection));
    }

    return standardSyncs;
  }

  @SuppressWarnings("LineLength")
  private Map<UUID, List<StandardSync>> getWorkspaceIdToStandardSyncsFromResult(final Result<Record> connectionAndOperationIdsResult,
                                                                                final List<NotificationConfigurationRecord> allNeededNotificationConfigurations) {
    final Map<UUID, List<StandardSync>> workspaceIdToStandardSync = new HashMap<>();

    for (final Record record : connectionAndOperationIdsResult) {
      final String operationIdsFromRecord = record.get(OPERATION_IDS_AGG_FIELD, String.class);

      // can be null when connection has no connectionOperations
      final List<UUID> operationIds = operationIdsFromRecord == null
          ? Collections.emptyList()
          : Arrays.stream(operationIdsFromRecord.split(OPERATION_IDS_AGG_DELIMITER)).map(UUID::fromString).toList();

      final UUID connectionId = record.get(CONNECTION.ID);
      final List<NotificationConfigurationRecord> notificationConfigurationsForConnection = allNeededNotificationConfigurations.stream()
          .filter(notificationConfiguration -> notificationConfiguration.getConnectionId().equals(connectionId))
          .toList();
      workspaceIdToStandardSync.computeIfAbsent(
          record.get(ACTOR.WORKSPACE_ID), v -> new ArrayList<>())
          .add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection));
    }

    return workspaceIdToStandardSync;
  }

  private Stream<StandardSyncOperation> listStandardSyncOperationQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(OPERATION);
      if (configId.isPresent()) {
        return query.where(OPERATION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    return result.map(ConfigRepository::buildStandardSyncOperation).stream();
  }

  private static StandardSyncOperation buildStandardSyncOperation(final Record record) {
    return new StandardSyncOperation()
        .withOperationId(record.get(OPERATION.ID))
        .withName(record.get(OPERATION.NAME))
        .withWorkspaceId(record.get(OPERATION.WORKSPACE_ID))
        .withOperatorType(Enums.toEnum(record.get(OPERATION.OPERATOR_TYPE, String.class), OperatorType.class).orElseThrow())
        .withOperatorNormalization(Jsons.deserialize(record.get(OPERATION.OPERATOR_NORMALIZATION).data(), OperatorNormalization.class))
        .withOperatorDbt(Jsons.deserialize(record.get(OPERATION.OPERATOR_DBT).data(), OperatorDbt.class))
        .withOperatorWebhook(record.get(OPERATION.OPERATOR_WEBHOOK) == null ? null
            : Jsons.deserialize(record.get(OPERATION.OPERATOR_WEBHOOK).data(), OperatorWebhook.class))
        .withTombstone(record.get(OPERATION.TOMBSTONE));
  }

  /**
   * Get sync operation.
   *
   * @param operationId operation id
   * @return sync operation
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public StandardSyncOperation getStandardSyncOperation(final UUID operationId) throws JsonValidationException, IOException, ConfigNotFoundException {
    return listStandardSyncOperationQuery(Optional.of(operationId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC_OPERATION, operationId));
  }

  /**
   * Write standard sync operation.
   *
   * @param standardSyncOperation standard sync operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  public void writeStandardSyncOperation(final StandardSyncOperation standardSyncOperation) throws IOException {
    database.transaction(ctx -> {
      writeStandardSyncOperation(Collections.singletonList(standardSyncOperation), ctx);
      return null;
    });
  }

  private void writeStandardSyncOperation(final List<StandardSyncOperation> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((standardSyncOperation) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(OPERATION)
          .where(OPERATION.ID.eq(standardSyncOperation.getOperationId())));

      if (isExistingConfig) {
        ctx.update(OPERATION)
            .set(OPERATION.ID, standardSyncOperation.getOperationId())
            .set(OPERATION.WORKSPACE_ID, standardSyncOperation.getWorkspaceId())
            .set(OPERATION.NAME, standardSyncOperation.getName())
            .set(OPERATION.OPERATOR_TYPE, Enums.toEnum(standardSyncOperation.getOperatorType().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType.class).orElseThrow())
            .set(OPERATION.OPERATOR_NORMALIZATION, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorNormalization())))
            .set(OPERATION.OPERATOR_DBT, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorDbt())))
            .set(OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorWebhook())))
            .set(OPERATION.TOMBSTONE, standardSyncOperation.getTombstone() != null && standardSyncOperation.getTombstone())
            .set(OPERATION.UPDATED_AT, timestamp)
            .where(OPERATION.ID.eq(standardSyncOperation.getOperationId()))
            .execute();

      } else {
        ctx.insertInto(OPERATION)
            .set(OPERATION.ID, standardSyncOperation.getOperationId())
            .set(OPERATION.WORKSPACE_ID, standardSyncOperation.getWorkspaceId())
            .set(OPERATION.NAME, standardSyncOperation.getName())
            .set(OPERATION.OPERATOR_TYPE, Enums.toEnum(standardSyncOperation.getOperatorType().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType.class).orElseThrow())
            .set(OPERATION.OPERATOR_NORMALIZATION, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorNormalization())))
            .set(OPERATION.OPERATOR_DBT, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorDbt())))
            .set(OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorWebhook())))
            .set(OPERATION.TOMBSTONE, standardSyncOperation.getTombstone() != null && standardSyncOperation.getTombstone())
            .set(OPERATION.CREATED_AT, timestamp)
            .set(OPERATION.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  /**
   * List standard sync operations.
   *
   * @return standard sync operations.
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StandardSyncOperation> listStandardSyncOperations() throws IOException {
    return listStandardSyncOperationQuery(Optional.empty()).toList();
  }

  /**
   * Updates {@link io.airbyte.db.instance.configs.jooq.generated.tables.ConnectionOperation} records
   * for the given {@code connectionId}.
   *
   * @param connectionId ID of the associated connection to update operations for
   * @param newOperationIds Set of all operationIds that should be associated to the connection
   * @throws IOException - exception while interacting with the db
   */
  public void updateConnectionOperationIds(final UUID connectionId, final Set<UUID> newOperationIds) throws IOException {
    database.transaction(ctx -> {
      final Set<UUID> existingOperationIds = ctx
          .selectFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
          .fetchSet(CONNECTION_OPERATION.OPERATION_ID);

      final Set<UUID> existingOperationIdsToKeep = Sets.intersection(existingOperationIds, newOperationIds);

      // DELETE existing connection_operation records that aren't in the input list
      final Set<UUID> operationIdsToDelete = Sets.difference(existingOperationIds, existingOperationIdsToKeep);

      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
          .and(CONNECTION_OPERATION.OPERATION_ID.in(operationIdsToDelete))
          .execute();

      // INSERT connection_operation records that are in the input list and don't yet exist
      final Set<UUID> operationIdsToAdd = Sets.difference(newOperationIds, existingOperationIdsToKeep);

      operationIdsToAdd.forEach(operationId -> ctx
          .insertInto(CONNECTION_OPERATION)
          .columns(CONNECTION_OPERATION.ID, CONNECTION_OPERATION.CONNECTION_ID, CONNECTION_OPERATION.OPERATION_ID)
          .values(UUID.randomUUID(), connectionId, operationId)
          .execute());

      return null;
    });
  }

  /**
   * Delete standard sync operation.
   *
   * @param standardSyncOperationId standard sync operation id
   * @throws IOException if there is an issue while interacting with db.
   */
  public void deleteStandardSyncOperation(final UUID standardSyncOperationId) throws IOException {
    database.transaction(ctx -> {
      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.OPERATION_ID.eq(standardSyncOperationId)).execute();
      ctx.update(OPERATION)
          .set(OPERATION.TOMBSTONE, true)
          .where(OPERATION.ID.eq(standardSyncOperationId)).execute();
      return null;
    });
  }

  private Stream<SourceOAuthParameter> listSourceOauthParamQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      if (configId.isPresent()) {
        return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source), ACTOR_OAUTH_PARAMETER.ID.eq(configId.get())).fetch();
      }
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source)).fetch();
    });

    return result.map(DbConverter::buildSourceOAuthParameter).stream();
  }

  /**
   * Get source oauth parameter.
   *
   * @param workspaceId workspace id
   * @param sourceDefinitionId source definition id
   * @return source oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  public Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(sourceDefinitionId)).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildSourceOAuthParameter);
  }

  /**
   * Write source oauth param.
   *
   * @param sourceOAuthParameter source oauth param
   * @throws IOException if there is an issue while interacting with db.
   */
  public void writeSourceOAuthParam(final SourceOAuthParameter sourceOAuthParameter) throws IOException {
    database.transaction(ctx -> {
      writeSourceOauthParameter(Collections.singletonList(sourceOAuthParameter), ctx);
      return null;
    });
  }

  private void writeSourceOauthParameter(final List<SourceOAuthParameter> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((sourceOAuthParameter) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR_OAUTH_PARAMETER)
          .where(ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.getOauthParameterId())));

      if (isExistingConfig) {
        ctx.update(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.getSourceDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.getOauthParameterId()))
            .execute();
      } else {
        ctx.insertInto(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.getSourceDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  /**
   * List source oauth parameters.
   *
   * @return oauth parameters
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<SourceOAuthParameter> listSourceOAuthParam() throws JsonValidationException, IOException {
    return listSourceOauthParamQuery(Optional.empty()).toList();
  }

  /**
   * List destination oauth param query. If configId is present only returns the config for that oauth
   * parameter id. if not present then lists all.
   *
   * @param configId oauth parameter id optional.
   * @return stream of destination oauth params
   * @throws IOException if there is an issue while interacting with db.
   */
  private Stream<DestinationOAuthParameter> listDestinationOauthParamQuery(final Optional<UUID> configId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      if (configId.isPresent()) {
        return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination), ACTOR_OAUTH_PARAMETER.ID.eq(configId.get())).fetch();
      }
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination)).fetch();
    });

    return result.map(DbConverter::buildDestinationOAuthParameter).stream();
  }

  /**
   * Get destination oauth parameter.
   *
   * @param workspaceId workspace id
   * @param destinationDefinitionId destination definition id
   * @return oauth parameters if present
   * @throws IOException if there is an issue while interacting with db.
   */
  public Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(final UUID workspaceId,
                                                                                            final UUID destinationDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(destinationDefinitionId)).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildDestinationOAuthParameter);
  }

  /**
   * Write destination oauth param.
   *
   * @param destinationOAuthParameter destination oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  public void writeDestinationOAuthParam(final DestinationOAuthParameter destinationOAuthParameter) throws IOException {
    database.transaction(ctx -> {
      writeDestinationOauthParameter(Collections.singletonList(destinationOAuthParameter), ctx);
      return null;
    });
  }

  private void writeDestinationOauthParameter(final List<DestinationOAuthParameter> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((destinationOAuthParameter) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR_OAUTH_PARAMETER)
          .where(ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.getOauthParameterId())));

      if (isExistingConfig) {
        ctx.update(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.getDestinationDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.getOauthParameterId()))
            .execute();

      } else {
        ctx.insertInto(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.getDestinationDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute();
      }
    });

  }

  /**
   * List destination oauth params.
   *
   * @return list destination oauth params
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<DestinationOAuthParameter> listDestinationOAuthParam() throws JsonValidationException, IOException {
    return listDestinationOauthParamQuery(Optional.empty()).toList();
  }

  private Map<UUID, AirbyteCatalog> findCatalogByHash(final String catalogHash, final DSLContext context) {
    final Result<Record2<UUID, JSONB>> records = context.select(ACTOR_CATALOG.ID, ACTOR_CATALOG.CATALOG)
        .from(ACTOR_CATALOG)
        .where(ACTOR_CATALOG.CATALOG_HASH.eq(catalogHash)).fetch();

    final Map<UUID, AirbyteCatalog> result = new HashMap<>();
    for (final Record record : records) {
      // We do not apply the on-the-fly migration here because the only caller is getOrInsertActorCatalog
      // which is using this to figure out if the catalog has already been inserted. Migrating on the fly
      // here will cause us to add a duplicate each time we check for existence of a catalog.
      final AirbyteCatalog catalog = Jsons.deserialize(record.get(ACTOR_CATALOG.CATALOG).toString(), AirbyteCatalog.class);
      result.put(record.get(ACTOR_CATALOG.ID), catalog);
    }
    return result;
  }

  /**
   * Updates the database with the most up-to-date source and destination definitions in the connector
   * catalog.
   *
   * @param seedSourceDefs - most up-to-date source definitions
   * @param seedDestDefs - most up-to-date destination definitions
   * @throws IOException - throws if exception when interacting with db
   */
  public void seedActorDefinitions(final List<StandardSourceDefinition> seedSourceDefs, final List<StandardDestinationDefinition> seedDestDefs)
      throws IOException {
    actorDefinitionMigrator.migrate(seedSourceDefs, seedDestDefs);
  }

  /**
   * Pair of source and its associated definition.
   *
   * Data-carrier records to hold combined result of query for a Source or Destination and its
   * corresponding Definition. This enables the API layer to process combined information about a
   * Source/Destination/Definition pair without requiring two separate queries and in-memory join
   * operation, because the config models are grouped immediately in the repository layer.
   *
   * @param source source
   * @param definition its corresponding definition
   */
  @VisibleForTesting
  public record SourceAndDefinition(SourceConnection source, StandardSourceDefinition definition) {

  }

  /**
   * Pair of destination and its associated definition.
   *
   * @param destination destination
   * @param definition its corresponding definition
   */
  @VisibleForTesting
  public record DestinationAndDefinition(DestinationConnection destination, StandardDestinationDefinition definition) {

  }

  /**
   * Get source and definition from sources ids.
   *
   * @param sourceIds source ids
   * @return pair of source and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<SourceAndDefinition> getSourceAndDefinitionsFromSourceIds(final List<UUID> sourceIds) throws IOException {
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
      final StandardSourceDefinition definition = DbConverter.buildStandardSourceDefinition(record, heartbeatMaxSecondBetweenMessageSupplier.get());
      sourceAndDefinitions.add(new SourceAndDefinition(source, definition));
    }

    return sourceAndDefinitions;
  }

  /**
   * Get destination and definition from destinations ids.
   *
   * @param destinationIds destination ids
   * @return pair of destination and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<DestinationAndDefinition> getDestinationAndDefinitionsFromDestinationIds(final List<UUID> destinationIds) throws IOException {
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
   * Get actor catalog.
   *
   * @param actorCatalogId actor catalog id
   * @return actor catalog
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public ActorCatalog getActorCatalogById(final UUID actorCatalogId)
      throws IOException, ConfigNotFoundException {
    final Result<Record> result = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG).where(ACTOR_CATALOG.ID.eq(actorCatalogId))).fetch();

    if (result.size() > 0) {
      return DbConverter.buildActorCatalog(result.get(0));
    }
    throw new ConfigNotFoundException(ConfigSchema.ACTOR_CATALOG, actorCatalogId);
  }

  /**
   * Store an Airbyte catalog in DB if it is not present already.
   *
   * Checks in the config DB if the catalog is present already, if so returns it identifier. It is not
   * present, it is inserted in DB with a new identifier and that identifier is returned.
   *
   * @param airbyteCatalog An Airbyte catalog to cache
   * @param context - db context
   * @return the db identifier for the cached catalog.
   */
  private UUID getOrInsertActorCatalog(final AirbyteCatalog airbyteCatalog,
                                       final DSLContext context,
                                       final OffsetDateTime timestamp) {
    final HashFunction hashFunction = Hashing.murmur3_32_fixed();
    final String catalogHash = hashFunction.hashBytes(Jsons.serialize(airbyteCatalog).getBytes(
        Charsets.UTF_8)).toString();
    final Map<UUID, AirbyteCatalog> catalogs = findCatalogByHash(catalogHash, context);

    for (final Map.Entry<UUID, AirbyteCatalog> entry : catalogs.entrySet()) {
      if (entry.getValue().equals(airbyteCatalog)) {
        return entry.getKey();
      }
    }

    final UUID catalogId = UUID.randomUUID();
    context.insertInto(ACTOR_CATALOG)
        .set(ACTOR_CATALOG.ID, catalogId)
        .set(ACTOR_CATALOG.CATALOG, JSONB.valueOf(Jsons.serialize(airbyteCatalog)))
        .set(ACTOR_CATALOG.CATALOG_HASH, catalogHash)
        .set(ACTOR_CATALOG.CREATED_AT, timestamp)
        .set(ACTOR_CATALOG.MODIFIED_AT, timestamp).execute();
    return catalogId;
  }

  /**
   * Get most actor catalog for source.
   *
   * @param actorId actor id
   * @param actorVersion actor definition version used to make this actor
   * @param configHash config hash for actor
   * @return actor catalog for config has and actor version
   * @throws IOException - error while interacting with db
   */
  public Optional<ActorCatalog> getActorCatalog(final UUID actorId,
                                                final String actorVersion,
                                                final String configHash)
      throws IOException {
    final Result<Record> records = database.transaction(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG).join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG.ID.eq(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(actorId))
        .and(ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION.eq(actorVersion))
        .and(ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH.eq(configHash))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1)).fetch();

    return records.stream().findFirst().map(DbConverter::buildActorCatalog);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog with updated at
   * @throws IOException - error while interacting with db
   */
  public Optional<ActorCatalogWithUpdatedAt> getMostRecentSourceActorCatalog(final UUID sourceId) throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk(), ACTOR_CATALOG_FETCH_EVENT.CREATED_AT)
        .from(ACTOR_CATALOG)
        .join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(ACTOR_CATALOG.ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalogWithUpdatedAt);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog
   * @throws IOException - error while interacting with db
   */
  public Optional<ActorCatalog> getMostRecentActorCatalogForSource(final UUID sourceId) throws IOException {
    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG.asterisk())
        .from(ACTOR_CATALOG)
        .join(ACTOR_CATALOG_FETCH_EVENT)
        .on(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID.eq(ACTOR_CATALOG.ID))
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalog);
  }

  /**
   * Get most recent actor catalog fetch event for source.
   *
   * @param sourceId source id
   * @return last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  public Optional<ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSource(final UUID sourceId) throws IOException {

    final Result<Record> records = database.query(ctx -> ctx.select(ACTOR_CATALOG_FETCH_EVENT.asterisk())
        .from(ACTOR_CATALOG_FETCH_EVENT)
        .where(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID.eq(sourceId))
        .orderBy(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT.desc()).limit(1).fetch());
    return records.stream().findFirst().map(DbConverter::buildActorCatalogFetchEvent);
  }

  /**
   * Get most recent actor catalog fetch event for sources.
   *
   * @param sourceIds source ids
   * @return map of source id to the last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  @SuppressWarnings({"unused", "SqlNoDataSourceInspection"})
  public Map<UUID, ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSources(final List<UUID> sourceIds) throws IOException {
    // noinspection SqlResolve
    if (sourceIds.isEmpty()) {
      return Collections.emptyMap();
    }
    return database.query(ctx -> ctx.fetch(
        """
        select distinct actor_catalog_id, actor_id, created_at from
          (select
            actor_catalog_id,
            actor_id,
            created_at,
            row_number() over (partition by actor_id order by created_at desc) as creation_order_row_number
          from public.actor_catalog_fetch_event
          where actor_id in ({0})
          ) table_with_rank
        where creation_order_row_number = 1;
        """,
        DSL.list(sourceIds.stream().map(DSL::value).collect(Collectors.toList()))))
        .stream().map(DbConverter::buildActorCatalogFetchEvent)
        .collect(Collectors.toMap(ActorCatalogFetchEvent::getActorId, record -> record));
  }

  /**
   * Stores source catalog information.
   *
   * This function is called each time the schema of a source is fetched. This can occur because the
   * source is set up for the first time, because the configuration or version of the connector
   * changed or because the user explicitly requested a schema refresh. Schemas are stored separately
   * and de-duplicated upon insertion. Once a schema has been successfully stored, a call to
   * getActorCatalog(actorId, connectionVersion, configurationHash) will return the most recent schema
   * stored for those parameters.
   *
   * @param catalog - catalog that was fetched.
   * @param actorId - actor the catalog was fetched by
   * @param connectorVersion - version of the connector when catalog was fetched
   * @param configurationHash - hash of the config of the connector when catalog was fetched
   * @return The identifier (UUID) of the fetch event inserted in the database
   * @throws IOException - error while interacting with db
   */
  public UUID writeActorCatalogFetchEvent(final AirbyteCatalog catalog,
                                          final UUID actorId,
                                          final String connectorVersion,
                                          final String configurationHash)
      throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final UUID fetchEventID = UUID.randomUUID();
    return database.transaction(ctx -> {
      final UUID catalogId = getOrInsertActorCatalog(catalog, ctx, timestamp);
      ctx.insertInto(ACTOR_CATALOG_FETCH_EVENT)
          .set(ACTOR_CATALOG_FETCH_EVENT.ID, fetchEventID)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID, actorId)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID, catalogId)
          .set(ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH, configurationHash)
          .set(ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION, connectorVersion)
          .set(ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT, timestamp)
          .set(ACTOR_CATALOG_FETCH_EVENT.CREATED_AT, timestamp).execute();
      return catalogId;
    });
  }

  /**
   * Count connections in workspace.
   *
   * @param workspaceId workspace id
   * @return number of connections in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  public int countConnectionsForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(CONNECTION)
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .and(CONNECTION.STATUS.notEqual(StatusType.deprecated))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * Count sources in workspace.
   *
   * @param workspaceId workspace id
   * @return number of sources in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  public int countSourcesForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .where(ACTOR.WORKSPACE_ID.equal(workspaceId))
        .and(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * Count destinations in workspace.
   *
   * @param workspaceId workspace id
   * @return number of destinations in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  public int countDestinationsForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .where(ACTOR.WORKSPACE_ID.equal(workspaceId))
        .and(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * The following methods are present to allow the JobCreationAndStatusUpdateActivity class to emit
   * metrics without exposing the underlying database connection.
   *
   * @param srcId source id
   * @param dstId destination id
   * @return release stages of source and destination
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<ReleaseStage> getSrcIdAndDestIdToReleaseStages(final UUID srcId, final UUID dstId) throws IOException {
    return database.query(ctx -> MetricQueries.srcIdAndDestIdToReleaseStages(ctx, srcId, dstId));
  }

  /**
   * Get release stages for job id.
   *
   * @param jobId job id
   * @return release stages
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<ReleaseStage> getJobIdToReleaseStages(final long jobId) throws IOException {
    return database.query(ctx -> MetricQueries.jobIdToReleaseStages(ctx, jobId));
  }

  private Condition includeTombstones(final Field<Boolean> tombstoneField, final boolean includeTombstones) {
    if (includeTombstones) {
      return DSL.trueCondition();
    } else {
      return tombstoneField.eq(false);
    }
  }

  /**
   * Get workspace service account without secrets.
   *
   * @param workspaceId workspace id
   * @return workspace service account
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public WorkspaceServiceAccount getWorkspaceServiceAccountNoSecrets(final UUID workspaceId) throws IOException, ConfigNotFoundException {
    // breaking the pattern of doing a list query, because we never want to list this resource without
    // scoping by workspace id.
    return database.query(ctx -> ctx.select(asterisk()).from(WORKSPACE_SERVICE_ACCOUNT)
        .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceId))
        .fetch())
        .map(DbConverter::buildWorkspaceServiceAccount)
        .stream()
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.WORKSPACE_SERVICE_ACCOUNT, workspaceId));
  }

  /**
   * Write workspace service account with no secrets.
   *
   * @param workspaceServiceAccount workspace service account
   * @throws IOException if there is an issue while interacting with db.
   */
  public void writeWorkspaceServiceAccountNoSecrets(final WorkspaceServiceAccount workspaceServiceAccount) throws IOException {
    database.transaction(ctx -> {
      writeWorkspaceServiceAccount(Collections.singletonList(workspaceServiceAccount), ctx);
      return null;
    });
  }

  /**
   * Write workspace service account.
   *
   * @param configs list of workspace service account
   * @param ctx database context
   */
  private void writeWorkspaceServiceAccount(final List<WorkspaceServiceAccount> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((workspaceServiceAccount) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(WORKSPACE_SERVICE_ACCOUNT)
          .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceServiceAccount.getWorkspaceId())));

      if (isExistingConfig) {
        ctx.update(WORKSPACE_SERVICE_ACCOUNT)
            .set(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID, workspaceServiceAccount.getWorkspaceId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_ID, workspaceServiceAccount.getServiceAccountId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_EMAIL, workspaceServiceAccount.getServiceAccountEmail())
            .set(WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getJsonCredential())))
            .set(WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getHmacKey())))
            .set(WORKSPACE_SERVICE_ACCOUNT.UPDATED_AT, timestamp)
            .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceServiceAccount.getWorkspaceId()))
            .execute();
      } else {
        ctx.insertInto(WORKSPACE_SERVICE_ACCOUNT)
            .set(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID, workspaceServiceAccount.getWorkspaceId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_ID, workspaceServiceAccount.getServiceAccountId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_EMAIL, workspaceServiceAccount.getServiceAccountEmail())
            .set(WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getJsonCredential())))
            .set(WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getHmacKey())))
            .set(WORKSPACE_SERVICE_ACCOUNT.CREATED_AT, timestamp)
            .set(WORKSPACE_SERVICE_ACCOUNT.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  /**
   * Get all streams for connection.
   *
   * @param connectionId connection id
   * @return list of streams for connection
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public List<StreamDescriptor> getAllStreamsForConnection(final UUID connectionId) throws ConfigNotFoundException, IOException {
    return standardSyncPersistence.getAllStreamsForConnection(connectionId);
  }

  /**
   * Get configured catalog for connection.
   *
   * @param connectionId connection id
   * @return configured catalog
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  public ConfiguredAirbyteCatalog getConfiguredCatalogForConnection(final UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync standardSync = getStandardSync(connectionId);
    return standardSync.getCatalog();
  }

  /**
   * Get geography for a connection.
   *
   * @param connectionId connection id
   * @return geography
   * @throws IOException exception while interacting with the db
   */
  public Geography getGeographyForConnection(final UUID connectionId) throws IOException {
    return database.query(ctx -> ctx.select(CONNECTION.GEOGRAPHY)
        .from(CONNECTION)
        .where(CONNECTION.ID.eq(connectionId))
        .limit(1))
        .fetchOneInto(Geography.class);
  }

  /**
   * Get geography for a workspace.
   *
   * @param workspaceId workspace id
   * @return geography
   * @throws IOException exception while interacting with the db
   */
  public Geography getGeographyForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.GEOGRAPHY)
        .from(WORKSPACE)
        .where(WORKSPACE.ID.eq(workspaceId))
        .limit(1))
        .fetchOneInto(Geography.class);
  }

  /**
   * Specialized query for efficiently determining eligibility for the Free Connector Program. If a
   * workspace has at least one Alpha or Beta connector, users of that workspace will be prompted to
   * sign up for the program. This check is performed on nearly every page load so the query needs to
   * be as efficient as possible.
   *
   * @param workspaceId ID of the workspace to check connectors for
   * @return boolean indicating if an alpha or beta connector exists within the workspace
   */
  public boolean getWorkspaceHasAlphaOrBetaConnector(final UUID workspaceId) throws IOException {
    final Condition releaseStageAlphaOrBeta = ACTOR_DEFINITION.RELEASE_STAGE.eq(ReleaseStage.alpha)
        .or(ACTOR_DEFINITION.RELEASE_STAGE.eq(ReleaseStage.beta));

    final Integer countResult = database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .join(ACTOR_DEFINITION).on(ACTOR_DEFINITION.ID.eq(ACTOR.ACTOR_DEFINITION_ID))
        .where(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .and(ACTOR.TOMBSTONE.notEqual(true))
        .and(releaseStageAlphaOrBeta))
        .fetchOneInto(Integer.class);

    return countResult > 0;
  }

  /**
   * Specialized query for efficiently determining a connection's eligibility for the Free Connector
   * Program. If a connection has at least one Alpha or Beta connector, it will be free to use as long
   * as the workspace is enrolled in the Free Connector Program. This check is used to allow free
   * connections to continue running even when a workspace runs out of credits.
   *
   * @param connectionId ID of the connection to check connectors for
   * @return boolean indicating if an alpha or beta connector is used by the connection
   */
  public boolean getConnectionHasAlphaOrBetaConnector(final UUID connectionId) throws IOException {
    final Condition releaseStageAlphaOrBeta = ACTOR_DEFINITION.RELEASE_STAGE.eq(ReleaseStage.alpha)
        .or(ACTOR_DEFINITION.RELEASE_STAGE.eq(ReleaseStage.beta));

    final Integer countResult = database.query(ctx -> ctx.selectCount()
        .from(CONNECTION)
        .join(ACTOR).on(ACTOR.ID.eq(CONNECTION.SOURCE_ID).or(ACTOR.ID.eq(CONNECTION.DESTINATION_ID)))
        .join(ACTOR_DEFINITION).on(ACTOR_DEFINITION.ID.eq(ACTOR.ACTOR_DEFINITION_ID))
        .where(CONNECTION.ID.eq(connectionId))
        .and(releaseStageAlphaOrBeta))
        .fetchOneInto(Integer.class);

    return countResult > 0;
  }

  /**
   * Get connector builder project.
   *
   * @param builderProjectId project id
   * @param fetchManifestDraft manifest draft
   * @return builder project
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException if build project is not found
   */
  public ConnectorBuilderProject getConnectorBuilderProject(final UUID builderProjectId, final boolean fetchManifestDraft)
      throws IOException, ConfigNotFoundException {
    final Optional<ConnectorBuilderProject> projectOptional = database.query(ctx -> {
      final List columnsToFetch =
          new ArrayList(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS);
      if (fetchManifestDraft) {
        columnsToFetch.add(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT);
      }
      final RecordMapper<Record, ConnectorBuilderProject> connectionBuilderProjectRecordMapper =
          fetchManifestDraft ? DbConverter::buildConnectorBuilderProject : DbConverter::buildConnectorBuilderProjectWithoutManifestDraft;
      return ctx.select(columnsToFetch)
          .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
          .from(CONNECTOR_BUILDER_PROJECT)
          .leftJoin(ACTIVE_DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
          .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId).andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
          .fetch()
          .map(connectionBuilderProjectRecordMapper)
          .stream()
          .findFirst();
    });
    return projectOptional.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.CONNECTOR_BUILDER_PROJECT, builderProjectId));
  }

  /**
   * Return a versioned manifest associated with a builder project.
   *
   * @param builderProjectId ID of the connector_builder_project
   * @param version the version of the manifest
   * @return ConnectorBuilderProjectVersionedManifest matching the builderProjectId
   * @throws ConfigNotFoundException ensures that there a connector_builder_project matching the
   *         `builderProjectId`, a declarative_manifest with the specified version associated with the
   *         builder project and an active_declarative_manifest. If either of these conditions is not
   *         true, this error is thrown
   * @throws IOException exception while interacting with db
   */
  public ConnectorBuilderProjectVersionedManifest getVersionedConnectorBuilderProject(final UUID builderProjectId, final Long version)
      throws ConfigNotFoundException, IOException {
    final Optional<ConnectorBuilderProjectVersionedManifest> projectOptional = database.query(ctx -> ctx
        .select(CONNECTOR_BUILDER_PROJECT.ID,
            CONNECTOR_BUILDER_PROJECT.NAME,
            CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID,
            field(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull()).as("hasDraft"))
        .select(DECLARATIVE_MANIFEST.VERSION, DECLARATIVE_MANIFEST.DESCRIPTION, DECLARATIVE_MANIFEST.MANIFEST)
        .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
        .from(CONNECTOR_BUILDER_PROJECT)
        .join(ACTIVE_DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
        .join(DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
        .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId)
            .andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE)
            .and(DECLARATIVE_MANIFEST.VERSION.eq(version)))
        .fetch()
        .map(ConfigRepository::buildConnectorBuilderProjectVersionedManifest)
        .stream()
        .findFirst());
    return projectOptional.orElseThrow(() -> new ConfigNotFoundException(
        "CONNECTOR_BUILDER_PROJECTS/DECLARATIVE_MANIFEST/ACTIVE_DECLARATIVE_MANIFEST",
        String.format("connector_builder_projects.id:%s manifest_version:%s", builderProjectId, version)));
  }

  private static ConnectorBuilderProjectVersionedManifest buildConnectorBuilderProjectVersionedManifest(final Record record) {
    return new ConnectorBuilderProjectVersionedManifest()
        .withName(record.get(CONNECTOR_BUILDER_PROJECT.NAME))
        .withBuilderProjectId(record.get(CONNECTOR_BUILDER_PROJECT.ID))
        .withHasDraft((Boolean) record.get("hasDraft"))
        .withSourceDefinitionId(record.get(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID))
        .withActiveDeclarativeManifestVersion(record.get(ACTIVE_DECLARATIVE_MANIFEST.VERSION))
        .withManifest(Jsons.deserialize(record.get(DECLARATIVE_MANIFEST.MANIFEST).data()))
        .withManifestVersion(record.get(DECLARATIVE_MANIFEST.VERSION))
        .withManifestDescription(record.get(DECLARATIVE_MANIFEST.DESCRIPTION));
  }

  /**
   * Get connector builder project from a workspace id.
   *
   * @param workspaceId workspace id
   * @return builder project
   * @throws IOException exception while interacting with db
   */
  public Stream<ConnectorBuilderProject> getConnectorBuilderProjectsByWorkspace(@Nonnull final UUID workspaceId) throws IOException {
    final Condition matchByWorkspace = CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.eq(workspaceId);

    return database
        .query(ctx -> ctx
            .select(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS)
            .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
            .from(CONNECTOR_BUILDER_PROJECT)
            .leftJoin(ACTIVE_DECLARATIVE_MANIFEST)
            .on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .where(matchByWorkspace.andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
            .orderBy(CONNECTOR_BUILDER_PROJECT.NAME.asc())
            .fetch())
        .map(DbConverter::buildConnectorBuilderProjectWithoutManifestDraft)
        .stream();
  }

  /**
   * Delete builder project.
   *
   * @param builderProjectId builder project to delete
   * @return true if successful
   * @throws IOException exception while interacting with db
   */
  public boolean deleteBuilderProject(final UUID builderProjectId) throws IOException {
    return database.transaction(ctx -> ctx.update(CONNECTOR_BUILDER_PROJECT).set(CONNECTOR_BUILDER_PROJECT.TOMBSTONE, true)
        .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId)).execute()) > 0;
  }

  /**
   * Write name and draft of a builder project. If it doesn't exist under the specified id, it is
   * created.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @throws IOException exception while interacting with db
   */
  public void writeBuilderProjectDraft(final UUID projectId, final UUID workspaceId, final String name, final JsonNode manifestDraft)
      throws IOException {
    database.transaction(ctx -> {
      writeBuilderProjectDraft(projectId, workspaceId, name, manifestDraft, ctx);
      return null;
    });
  }

  private void writeBuilderProjectDraft(final UUID projectId,
                                        final UUID workspaceId,
                                        final String name,
                                        final JsonNode manifestDraft,
                                        final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchId = CONNECTOR_BUILDER_PROJECT.ID.eq(projectId);
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(CONNECTOR_BUILDER_PROJECT)
        .where(matchId));

    if (isExistingConfig) {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
              manifestDraft != null ? JSONB.valueOf(Jsons.serialize(manifestDraft)) : null)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .where(matchId)
          .execute();
    } else {
      ctx.insertInto(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
              manifestDraft != null ? JSONB.valueOf(Jsons.serialize(manifestDraft)) : null)
          .set(CONNECTOR_BUILDER_PROJECT.CREATED_AT, timestamp)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .set(CONNECTOR_BUILDER_PROJECT.TOMBSTONE, false)
          .execute();
    }
  }

  /**
   * Write name and draft of a builder project. The actor_definition is also updated to match the new
   * builder project name.
   *
   * Actor definition updated this way should always be private (i.e. public=false). As an additional
   * protection, we want to shield ourselves from users updating public actor definition and
   * therefore, the name of the actor definition won't be updated if the actor definition is not
   * public. See
   * https://github.com/airbytehq/airbyte-platform-internal/pull/5289#discussion_r1142757109.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @param actorDefinitionId the id of the associated actor definition
   * @throws IOException exception while interacting with db
   */
  public void updateBuilderProjectAndActorDefinition(final UUID projectId,
                                                     final UUID workspaceId,
                                                     final String name,
                                                     final JsonNode manifestDraft,
                                                     final UUID actorDefinitionId)
      throws IOException {
    database.transaction(ctx -> {
      writeBuilderProjectDraft(projectId, workspaceId, name, manifestDraft, ctx);
      ctx.update(ACTOR_DEFINITION)
          .set(ACTOR_DEFINITION.NAME, name)
          .where(ACTOR_DEFINITION.ID.eq(actorDefinitionId).and(ACTOR_DEFINITION.PUBLIC.eq(false)))
          .execute();
      return null;
    });
  }

  /**
   * Write a builder project to the db.
   *
   * @param builderProjectId builder project to update
   * @param actorDefinitionId the actor definition id associated with the connector builder project
   * @throws IOException exception while interacting with db
   */
  public void assignActorDefinitionToConnectorBuilderProject(final UUID builderProjectId, final UUID actorDefinitionId) throws IOException {
    database.transaction(ctx -> {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID, actorDefinitionId)
          .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId))
          .execute();
      return null;
    });
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   *
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   *
   * Since we decided not to validate this using the signature of the method, we will validate that
   * runtime and IllegalArgumentException if there is a mismatch.
   *
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   *
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param declarativeManifest declarative manifest version to create and make active
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being created
   * @throws IOException exception while interacting with db
   * @throws IllegalArgumentException if there is a mismatch between the different arguments
   */
  public void createDeclarativeManifestAsActiveVersion(final DeclarativeManifest declarativeManifest,
                                                       final ActorDefinitionConfigInjection configInjection,
                                                       final ConnectorSpecification connectorSpecification)
      throws IOException {
    if (!declarativeManifest.getActorDefinitionId().equals(configInjection.getActorDefinitionId())) {
      throw new IllegalArgumentException("DeclarativeManifest.actorDefinitionId must match ActorDefinitionConfigInjection.actorDefinitionId");
    }
    if (!declarativeManifest.getManifest().equals(configInjection.getJsonToInject())) {
      throw new IllegalArgumentException("The DeclarativeManifest does not match the config injection");
    }
    if (!declarativeManifest.getSpec().get("connectionSpecification").equals(connectorSpecification.getConnectionSpecification())) {
      throw new IllegalArgumentException("DeclarativeManifest.spec must match ConnectorSpecification.connectionSpecification");
    }

    database.transaction(ctx -> {
      updateDeclarativeActorDefinition(configInjection, connectorSpecification, ctx);
      insertActiveDeclarativeManifest(declarativeManifest, ctx);
      return null;
    });
  }

  private void upsertActiveDeclarativeManifest(final ActiveDeclarativeManifest activeDeclarativeManifest, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchId = ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(activeDeclarativeManifest.getActorDefinitionId());
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ACTIVE_DECLARATIVE_MANIFEST)
        .where(matchId));

    if (isExistingConfig) {
      ctx.update(ACTIVE_DECLARATIVE_MANIFEST)
          .set(ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.getVersion())
          .set(ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .where(matchId)
          .execute();
    } else {
      ctx.insertInto(ACTIVE_DECLARATIVE_MANIFEST)
          .set(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, activeDeclarativeManifest.getActorDefinitionId())
          .set(ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.getVersion())
          .set(ACTIVE_DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
          .set(ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   *
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   *
   * At that point, we can only hope the user won't cause data consistency issue using this method
   *
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   *
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param sourceDefinitionId actor definition to update
   * @param version the version of the manifest to make active. declarative_manifest.version must
   *        already exist
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being made active
   * @throws IOException exception while interacting with db
   */
  public void setDeclarativeSourceActiveVersion(final UUID sourceDefinitionId,
                                                final Long version,
                                                final ActorDefinitionConfigInjection configInjection,
                                                final ConnectorSpecification connectorSpecification)
      throws IOException {
    database.transaction(ctx -> {
      updateDeclarativeActorDefinition(configInjection, connectorSpecification, ctx);
      upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(sourceDefinitionId).withVersion(version), ctx);
      return null;
    });
  }

  /**
   * Load all config injection for an actor definition.
   *
   * @param actorDefinitionId id of the actor definition to fetch
   * @return stream of config injection objects
   * @throws IOException exception while interacting with db
   */
  public Stream<ActorDefinitionConfigInjection> getActorDefinitionConfigInjections(final UUID actorDefinitionId) throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION_CONFIG_INJECTION.asterisk())
        .from(ACTOR_DEFINITION_CONFIG_INJECTION)
        .where(ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch())
        .map(DbConverter::buildActorDefinitionConfigInjection)
        .stream();
  }

  /**
   * Update or create a config injection object. If there is an existing config injection for the
   * given actor definition and path, it is updated. If there isn't yet, a new config injection is
   * created.
   *
   * @param actorDefinitionConfigInjection the config injection object to write to the database
   * @throws IOException exception while interacting with db
   */
  public void writeActorDefinitionConfigInjectionForPath(final ActorDefinitionConfigInjection actorDefinitionConfigInjection) throws IOException {
    database.transaction(ctx -> {
      writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection, ctx);
      return null;
    });
  }

  private void writeActorDefinitionConfigInjectionForPath(final ActorDefinitionConfigInjection actorDefinitionConfigInjection, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchActorDefinitionIdAndInjectionPath =
        ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionConfigInjection.getActorDefinitionId())
            .and(ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH.eq(actorDefinitionConfigInjection.getInjectionPath()));
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ACTOR_DEFINITION_CONFIG_INJECTION)
        .where(matchActorDefinitionIdAndInjectionPath));

    if (isExistingConfig) {
      ctx.update(ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT, JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.getJsonToInject())))
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .where(matchActorDefinitionIdAndInjectionPath)
          .execute();
    } else {
      ctx.insertInto(ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH, actorDefinitionConfigInjection.getInjectionPath())
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID, actorDefinitionConfigInjection.getActorDefinitionId())
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT, JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.getJsonToInject())))
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.CREATED_AT, timestamp)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * Insert a declarative manifest. If DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and
   * DECLARATIVE_MANIFEST.VERSION is already in the DB, an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  public void insertDeclarativeManifest(final DeclarativeManifest declarativeManifest) throws IOException {
    database.transaction(ctx -> {
      insertDeclarativeManifest(declarativeManifest, ctx);
      return null;
    });
  }

  private static void insertDeclarativeManifest(final DeclarativeManifest declarativeManifest, final DSLContext ctx) {
    // Since "null" is a valid JSON object, `JSONB.valueOf(Jsons.serialize(null))` returns a valid JSON
    // object that is not null. Therefore, we will validate null values for JSON fields here
    if (declarativeManifest.getManifest() == null) {
      throw new DataAccessException("null value in column \"manifest\" of relation \"declarative_manifest\" violates not-null constraint");
    }
    if (declarativeManifest.getSpec() == null) {
      throw new DataAccessException("null value in column \"spec\" of relation \"declarative_manifest\" violates not-null constraint");
    }

    final OffsetDateTime timestamp = OffsetDateTime.now();
    ctx.insertInto(DECLARATIVE_MANIFEST)
        .set(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, declarativeManifest.getActorDefinitionId())
        .set(DECLARATIVE_MANIFEST.DESCRIPTION, declarativeManifest.getDescription())
        .set(DECLARATIVE_MANIFEST.MANIFEST, JSONB.valueOf(Jsons.serialize(declarativeManifest.getManifest())))
        .set(DECLARATIVE_MANIFEST.SPEC, JSONB.valueOf(Jsons.serialize(declarativeManifest.getSpec())))
        .set(DECLARATIVE_MANIFEST.VERSION, declarativeManifest.getVersion())
        .set(DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
        .execute();
  }

  /**
   * Insert a declarative manifest and its associated active declarative manifest. If
   * DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and DECLARATIVE_MANIFEST.VERSION is already in the DB,
   * an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  public void insertActiveDeclarativeManifest(final DeclarativeManifest declarativeManifest) throws IOException {
    database.transaction(ctx -> {
      insertDeclarativeManifest(declarativeManifest, ctx);
      upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(declarativeManifest.getActorDefinitionId())
          .withVersion(declarativeManifest.getVersion()), ctx);
      return null;
    });
  }

  private void insertActiveDeclarativeManifest(final DeclarativeManifest declarativeManifest, final DSLContext ctx) {
    insertDeclarativeManifest(declarativeManifest, ctx);
    upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(declarativeManifest.getActorDefinitionId())
        .withVersion(declarativeManifest.getVersion()), ctx);
  }

  /**
   * Read all declarative manifests by actor definition id without the manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   */
  public Stream<DeclarativeManifest> getDeclarativeManifestsByActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    return database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, DECLARATIVE_MANIFEST.DESCRIPTION, DECLARATIVE_MANIFEST.SPEC,
                DECLARATIVE_MANIFEST.VERSION)
            .from(DECLARATIVE_MANIFEST)
            .where(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch())
        .map(DbConverter::buildDeclarativeManifestWithoutManifestAndSpec)
        .stream();
  }

  /**
   * Read declarative manifest by actor definition id and version with manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @param version the version of the declarative manifest
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         and DECLARATIVE_MANIFEST.VERSION
   */
  public DeclarativeManifest getDeclarativeManifestByActorDefinitionIdAndVersion(final UUID actorDefinitionId, final long version)
      throws IOException, ConfigNotFoundException {
    final Optional<DeclarativeManifest> declarativeManifest = database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.asterisk())
            .from(DECLARATIVE_MANIFEST)
            .where(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId).and(DECLARATIVE_MANIFEST.VERSION.eq(version)))
            .fetch())
        .map(DbConverter::buildDeclarativeManifest)
        .stream().findFirst();
    return declarativeManifest.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DECLARATIVE_MANIFEST,
        String.format("actorDefinitionId:%s,version:%s", actorDefinitionId, version)));
  }

  /**
   * Read currently active declarative manifest by actor definition id by joining with
   * active_declarative_manifest for the same actor definition id with manifest.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         that matches the version of an active manifest
   */
  public DeclarativeManifest getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(final UUID actorDefinitionId)
      throws IOException, ConfigNotFoundException {
    final Optional<DeclarativeManifest> declarativeManifest = database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.asterisk())
            .from(DECLARATIVE_MANIFEST)
            .join(ACTIVE_DECLARATIVE_MANIFEST, JoinType.JOIN)
            .on(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID),
                DECLARATIVE_MANIFEST.VERSION.eq(ACTIVE_DECLARATIVE_MANIFEST.VERSION))
            .where(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch())
        .map(DbConverter::buildDeclarativeManifest)
        .stream().findFirst();
    return declarativeManifest.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DECLARATIVE_MANIFEST,
        String.format("ACTIVE_DECLARATIVE_MANIFEST.actor_definition_id:%s and matching DECLARATIVE_MANIFEST.version", actorDefinitionId)));
  }

  /**
   * Read all actor definition ids which have an active declarative manifest pointing to them.
   *
   * @throws IOException exception while interacting with db
   */
  public Stream<UUID> getActorDefinitionIdsWithActiveDeclarativeManifest() throws IOException {
    return database
        .query(ctx -> ctx
            .select(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID)
            .from(ACTIVE_DECLARATIVE_MANIFEST)
            .fetch())
        .stream().map(record -> record.get(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID));
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

  public static Supplier<Long> getMaxSecondsBetweenMessagesSupplier(final FeatureFlagClient featureFlagClient) {
    return () -> Long.parseLong(featureFlagClient.stringVariation(HeartbeatMaxSecondsBetweenMessages.INSTANCE, new Workspace(VOID_UUID)));
  }

  /**
   * Insert an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to insert
   * @throws IOException - you never know when you io
   */
  public void writeActorDefinitionVersion(final ActorDefinitionVersion actorDefinitionVersion) throws IOException {
    database.transaction(ctx -> ctx.insertInto(Tables.ACTOR_DEFINITION_VERSION))
        .set(Tables.ACTOR_DEFINITION_VERSION.ID, actorDefinitionVersion.getVersionId())
        .set(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, actorDefinitionVersion.getActorDefinitionId())
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, actorDefinitionVersion.getDockerRepository())
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, actorDefinitionVersion.getDockerImageTag())
        .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSpec())))
        // TODO (Ella): Replace the below null values with values from
        // actorDefinitionVersion once the POJO has these fields defined
        .setNull(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_REPOSITORY)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_TAG)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DBT)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_INTEGRATION_TYPE)
        .setNull(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS)
        .execute();
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
  public Optional<ActorDefinitionVersion> getActorDefinitionVersion(final UUID actorDefinitionId, final String dockerImageTag)
      throws IOException {
    return database.query(ctx -> ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION))
        .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(dockerImageTag)))
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion);
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
  public ActorDefinitionVersion getActorDefinitionVersion(final UUID actorDefinitionVersionId) throws IOException, ConfigNotFoundException {
    return database.query(ctx -> ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION))
        .where(Tables.ACTOR_DEFINITION_VERSION.ID.eq(actorDefinitionVersionId))
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion)
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.ACTOR_DEFINITION_VERSION, actorDefinitionVersionId.toString()));
  }

}
