/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.STATE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.groupConcat;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigWithMetadata;
import io.airbyte.config.Geography;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.TableField;
import org.jooq.impl.TableImpl;

@Singleton
public class ConnectionServiceJooqImpl implements ConnectionService {

  private static final String OPERATION_IDS_AGG_DELIMITER = ",";
  private static final String OPERATION_IDS_AGG_FIELD = "operation_ids_agg";

  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public ConnectionServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Deletes a connection (sync) and all of dependent resources (state and connection_operations).
   *
   * @param syncId - id of the sync (a.k.a. connection_id)
   * @throws IOException - error while accessing db.
   */
  @Override
  public void deleteStandardSync(UUID syncId) throws IOException {
    database.transaction(ctx -> {
      deleteConfig(NOTIFICATION_CONFIGURATION, NOTIFICATION_CONFIGURATION.CONNECTION_ID, syncId, ctx);
      deleteConfig(CONNECTION_OPERATION, CONNECTION_OPERATION.CONNECTION_ID, syncId, ctx);
      deleteConfig(STATE, STATE.CONNECTION_ID, syncId, ctx);
      deleteConfig(CONNECTION, CONNECTION.ID, syncId, ctx);
      return null;
    });
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
  @Override
  public StandardSync getStandardSync(UUID connectionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConfigWithMetadata<StandardSync>> result = listStandardSyncWithMetadata(Optional.of(connectionId));

    final boolean foundMoreThanOneConfig = result.size() > 1;
    if (result.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC, connectionId.toString());
    } else if (foundMoreThanOneConfig) {
      throw new IllegalStateException(String.format("Multiple %s configs found for ID %s: %s", ConfigSchema.STANDARD_SYNC, connectionId, result));
    }
    return result.get(0).getConfig();
  }

  /**
   * Write connection.
   *
   * @param standardSync connection
   * @throws IOException - exception while interacting with the db
   */
  @Override
  public void writeStandardSync(StandardSync standardSync) throws IOException {
    database.transaction(ctx -> {
      writeStandardSync(standardSync, ctx);
      return null;
    });
  }

  /**
   * List connections.
   *
   * @return connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listStandardSyncs() throws IOException {
    return listStandardSyncWithMetadata(Optional.empty()).stream().map(ConfigWithMetadata::getConfig).toList();
  }

  /**
   * List connections using operation.
   *
   * @param operationId operation id.
   * @return Connections that use the operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listStandardSyncsUsingOperation(UUID operationId) throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
        .from(CONNECTION)

        // inner join with all connection_operation rows that match the connection's id
        .join(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        // The schema management can be non-existent for a connection id, thus we need to do a left join
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        // only keep rows for connections that have an operationId that matches the input.
        // needs to be a sub query because we want to keep all operationIds for matching connections
        // in the main query
        .where(CONNECTION.ID.in(
            select(CONNECTION.ID).from(CONNECTION).join(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
                .where(CONNECTION_OPERATION.OPERATION_ID.eq(operationId))))

        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)).fetch();

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
  @Override
  public List<StandardSync> listWorkspaceStandardSyncs(UUID workspaceId, boolean includeDeleted)
      throws IOException {
    return listWorkspaceStandardSyncs(new StandardSyncQuery(workspaceId, null, null, includeDeleted));
  }

  /**
   * List connections for workspace via a query.
   *
   * @param standardSyncQuery query
   * @return list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listWorkspaceStandardSyncs(StandardSyncQuery standardSyncQuery)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
        .from(CONNECTION)

        // left join with all connection_operation rows that match the connection's id.
        // left join includes connections that don't have any connection_operations
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        // The schema management can be non-existent for a connection id, thus we need to do a left join
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        // join with source actors so that we can filter by workspaceId
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.eq(standardSyncQuery.workspaceId())
            .and(standardSyncQuery.destinationId() == null || standardSyncQuery.destinationId().isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncQuery.destinationId()))
            .and(standardSyncQuery.sourceId() == null || standardSyncQuery.sourceId().isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncQuery.sourceId()))
            .and(standardSyncQuery.includeDeleted() ? noCondition()
                : CONNECTION.STATUS.notEqual(
                    StatusType.deprecated)))

        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * List connections. Paginated.
   */
  @Override
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(List<UUID> workspaceIds,
                                                                           boolean includeDeleted,
                                                                           int pageSize,
                                                                           int rowOffset)
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
  @Override
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(
                                                                           StandardSyncsQueryPaginated standardSyncsQueryPaginated)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            ACTOR.WORKSPACE_ID,
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
        .from(CONNECTION)

        // left join with all connection_operation rows that match the connection's id.
        // left join includes connections that don't have any connection_operations
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))

        // join with source actors so that we can filter by workspaceId
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        // The schema management can be non-existent for a connection id, thus we need to do a left join
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(ACTOR.WORKSPACE_ID.in(standardSyncsQueryPaginated.workspaceIds())
            .and(standardSyncsQueryPaginated.destinationId() == null || standardSyncsQueryPaginated.destinationId().isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncsQueryPaginated.destinationId()))
            .and(standardSyncsQueryPaginated.sourceId() == null || standardSyncsQueryPaginated.sourceId().isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncsQueryPaginated.sourceId()))
            .and(standardSyncsQueryPaginated.includeDeleted() ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID, ACTOR.WORKSPACE_ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS))
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
  @Override
  public List<StandardSync> listConnectionsBySource(UUID sourceId, boolean includeDeleted)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.SOURCE_ID.eq(sourceId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * List connections that use a particular actor definition.
   *
   * @param actorDefinitionId id of the source or destination definition.
   * @param actorTypeValue either 'source' or 'destination' enum value.
   * @param includeDeleted whether to include tombstoned records in the return value.
   * @return List of connections that use the actor definition.
   * @throws IOException you never know when you IO
   */
  @Override
  public List<StandardSync> listConnectionsByActorDefinitionIdAndType(UUID actorDefinitionId,
                                                                      String actorTypeValue,
                                                                      boolean includeDeleted)
      throws IOException {
    final Condition actorDefinitionJoinCondition = switch (ActorType.valueOf(actorTypeValue)) {
      case source -> ACTOR.ACTOR_TYPE.eq(ActorType.source).and(ACTOR.ID.eq(CONNECTION.SOURCE_ID));
      case destination -> ACTOR.ACTOR_TYPE.eq(ActorType.destination).and(ACTOR.ID.eq(CONNECTION.DESTINATION_ID));
    };

    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(ACTOR).on(actorDefinitionJoinCondition)
        .leftJoin(SCHEMA_MANAGEMENT).on(CONNECTION.ID.eq(SCHEMA_MANAGEMENT.CONNECTION_ID))
        .where(ACTOR.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)).fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds));
  }

  /**
   * Get all streams for connection.
   *
   * @param connectionId connection id
   * @return list of streams for connection
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StreamDescriptor> getAllStreamsForConnection(UUID connectionId)
      throws ConfigNotFoundException, IOException {
    try {
      final StandardSync standardSync = getStandardSync(connectionId);
      return standardSync.getCatalog().getStreams().stream().map(CatalogHelpers::extractDescriptor).toList();
    } catch (JsonValidationException e) {
      throw new RuntimeException(e);
    }
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
  @Override
  public ConfiguredAirbyteCatalog getConfiguredCatalogForConnection(UUID connectionId)
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
  @Override
  public Geography getGeographyForConnection(UUID connectionId) throws IOException {
    return database.query(ctx -> ctx.select(CONNECTION.GEOGRAPHY)
        .from(CONNECTION)
        .where(CONNECTION.ID.eq(connectionId))
        .limit(1))
        .fetchOneInto(Geography.class);
  }

  /**
   * Specialized query for efficiently determining a connection's eligibility for the Free Connector
   * Program. If a connection has at least one Alpha or Beta connector, it will be free to use as long
   * as the workspace is enrolled in the Free Connector Program. This check is used to allow free
   * connections to continue running even when a workspace runs out of credits.
   * <p>
   * This should only be used for efficiently determining eligibility for the Free Connector Program.
   * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
   * the ReleaseStages.
   *
   * @param connectionId ID of the connection to check connectors for
   * @return boolean indicating if an alpha or beta connector is used by the connection
   */
  @Override
  public boolean getConnectionHasAlphaOrBetaConnector(UUID connectionId) throws IOException {
    final Condition releaseStageAlphaOrBeta = ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.alpha)
        .or(ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.beta));

    final Integer countResult = database.query(ctx -> ctx.selectCount()
        .from(CONNECTION)
        .join(ACTOR).on(ACTOR.ID.eq(CONNECTION.SOURCE_ID).or(ACTOR.ID.eq(CONNECTION.DESTINATION_ID)))
        .join(ACTOR_DEFINITION).on(ACTOR_DEFINITION.ID.eq(ACTOR.ACTOR_DEFINITION_ID))
        .join(ACTOR_DEFINITION_VERSION).on(ACTOR_DEFINITION_VERSION.ID.eq(ACTOR_DEFINITION.DEFAULT_VERSION_ID))
        .where(CONNECTION.ID.eq(connectionId))
        .and(releaseStageAlphaOrBeta))
        .fetchOneInto(Integer.class);

    return countResult > 0;
  }

  @Override
  public Set<Long> listEarlySyncJobs(int freeUsageInterval, int jobsFetchRange) throws IOException {
    return database.query(ctx -> getEarlySyncJobsFromResult(ctx.fetch(
        EARLY_SYNC_JOB_QUERY, freeUsageInterval, jobsFetchRange)));
  }

  /**
   * Disable a list of connections by setting their status to inactive.
   *
   * @param connectionIds list of connection ids to disable
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void disableConnectionsById(List<UUID> connectionIds) throws IOException {
    database.transaction(ctx -> {
      ctx.update(CONNECTION)
          .set(CONNECTION.UPDATED_AT, OffsetDateTime.now())
          .set(CONNECTION.STATUS, StatusType.inactive)
          .where(CONNECTION.ID.in(connectionIds))
          .execute();
      return null;
    });
  }

  private Set<Long> getEarlySyncJobsFromResult(final Result<Record> result) {
    // Transform the result to a list of early sync job ids
    // the rest of the fields are not used, we aim to keep the set small
    final Set<Long> earlySyncJobs = new HashSet<>();
    for (final Record record : result) {
      earlySyncJobs.add((Long) record.get("job_id"));
    }
    return earlySyncJobs;
  }

  /**
   * This query retrieves billable sync jobs (job status: INCOMPLETE, SUCCEEDED and CANCELLED) for
   * connections that have been created in the past 7 days OR finds the first successful sync jobs for
   * their corresponding connections. These results are used to mark these early syncs as free.
   */
  private static final String EARLY_SYNC_JOB_QUERY =
      // Find the first successful sync job ID for every connection.
      // This will be used in a join below to check if a particular job is the connection's
      // first successful sync
      "WITH FirstSuccessfulJobIdByConnection AS ("
          + " SELECT j2.scope, MIN(j2.id) AS min_job_id"
          + " FROM jobs j2"
          + " WHERE j2.status = 'succeeded' AND j2.config_type = 'sync'"
          + " GROUP BY j2.scope"
          + ")"
          // Left join Jobs on Connection and the above MinJobIds, and only keep billable
          // sync jobs that have an associated Connection ID
          + " SELECT j.id AS job_id, j.created_at, c.id AS conn_id, c.created_at AS connection_created_at, min_job_id"
          + " FROM jobs j"
          + " LEFT JOIN connection c ON c.id = UUID(j.scope)"
          + " LEFT JOIN FirstSuccessfulJobIdByConnection min_j_ids ON j.id = min_j_ids.min_job_id"
          + " WHERE j.status IN ('succeeded', 'incomplete', 'cancelled')"
          + " AND j.config_type = 'sync'"
          + " AND c.id IS NOT NULL"
          // Keep a job if it was created within 7 days of its connection's creation,
          // OR if it was the first successful sync job of its connection
          + " AND ((j.created_at < c.created_at + make_interval(days => ?))"
          + "      OR min_job_id IS NOT NULL)"
          // Only consider jobs that were created in the last 30 days, to cut down the query size.
          + " AND j.created_at > now() - make_interval(days => ?);";

  /**
   * Helper to delete records from the database.
   *
   * @param table the table to delete from
   * @param keyColumn the column to use as a key
   * @param configId the id of the object to delete, must be from the keyColumn
   * @param ctx the db context to use
   */
  private static <T extends Record> void deleteConfig(final TableImpl<T> table,
                                                      final TableField<T, UUID> keyColumn,
                                                      final UUID configId,
                                                      final DSLContext ctx) {
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(table)
        .where(keyColumn.eq(configId)));

    if (isExistingConfig) {
      ctx.deleteFrom(table)
          .where(keyColumn.eq(configId))
          .execute();
    }
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

  private void writeStandardSync(final StandardSync standardSync, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(CONNECTION)
        .where(CONNECTION.ID.eq(standardSync.getConnectionId())));

    if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
      throw new RuntimeException("unexpected schedule type mismatch");
    }

    if (isExistingConfig) {
      ctx.update(CONNECTION)
          .set(CONNECTION.ID, standardSync.getConnectionId())
          .set(CONNECTION.NAMESPACE_DEFINITION, Enums.toEnum(standardSync.getNamespaceDefinition().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType.class).orElseThrow())
          .set(CONNECTION.NAMESPACE_FORMAT, standardSync.getNamespaceFormat())
          .set(CONNECTION.PREFIX, standardSync.getPrefix())
          .set(CONNECTION.SOURCE_ID, standardSync.getSourceId())
          .set(CONNECTION.DESTINATION_ID, standardSync.getDestinationId())
          .set(CONNECTION.NAME, standardSync.getName())
          .set(CONNECTION.CATALOG, JSONB.valueOf(Jsons.serialize(standardSync.getCatalog())))
          .set(CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getFieldSelectionData())))
          .set(CONNECTION.STATUS, standardSync.getStatus() == null ? null
              : Enums.toEnum(standardSync.getStatus().value(),
                  io.airbyte.db.instance.configs.jooq.generated.enums.StatusType.class).orElseThrow())
          .set(CONNECTION.SCHEDULE, JSONB.valueOf(Jsons.serialize(standardSync.getSchedule())))
          .set(CONNECTION.MANUAL, standardSync.getManual())
          .set(CONNECTION.SCHEDULE_TYPE,
              standardSync.getScheduleType() == null ? null
                  : Enums.toEnum(standardSync.getScheduleType().value(),
                      io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.class)
                      .orElseThrow())
          .set(CONNECTION.SCHEDULE_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getScheduleData())))
          .set(CONNECTION.RESOURCE_REQUIREMENTS,
              JSONB.valueOf(Jsons.serialize(standardSync.getResourceRequirements())))
          .set(CONNECTION.UPDATED_AT, timestamp)
          .set(CONNECTION.SOURCE_CATALOG_ID, standardSync.getSourceCatalogId())
          .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
          .set(CONNECTION.GEOGRAPHY, Enums.toEnum(standardSync.getGeography().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
          .where(CONNECTION.ID.eq(standardSync.getConnectionId()))
          .execute();

      updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
      updateOrCreateSchemaChangeNotificationPreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(), timestamp,
          ctx);

      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
          .execute();

      for (final UUID operationIdFromStandardSync : standardSync.getOperationIds()) {
        ctx.insertInto(CONNECTION_OPERATION)
            .set(CONNECTION_OPERATION.ID, UUID.randomUUID())
            .set(CONNECTION_OPERATION.CONNECTION_ID, standardSync.getConnectionId())
            .set(CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
            .set(CONNECTION_OPERATION.CREATED_AT, timestamp)
            .set(CONNECTION_OPERATION.UPDATED_AT, timestamp)
            .execute();
      }
    } else {
      ctx.insertInto(CONNECTION)
          .set(CONNECTION.ID, standardSync.getConnectionId())
          .set(CONNECTION.NAMESPACE_DEFINITION, Enums.toEnum(standardSync.getNamespaceDefinition().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType.class).orElseThrow())
          .set(CONNECTION.NAMESPACE_FORMAT, standardSync.getNamespaceFormat())
          .set(CONNECTION.PREFIX, standardSync.getPrefix())
          .set(CONNECTION.SOURCE_ID, standardSync.getSourceId())
          .set(CONNECTION.DESTINATION_ID, standardSync.getDestinationId())
          .set(CONNECTION.NAME, standardSync.getName())
          .set(CONNECTION.CATALOG, JSONB.valueOf(Jsons.serialize(standardSync.getCatalog())))
          .set(CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getFieldSelectionData())))
          .set(CONNECTION.STATUS, standardSync.getStatus() == null ? null
              : Enums.toEnum(standardSync.getStatus().value(),
                  io.airbyte.db.instance.configs.jooq.generated.enums.StatusType.class).orElseThrow())
          .set(CONNECTION.SCHEDULE, JSONB.valueOf(Jsons.serialize(standardSync.getSchedule())))
          .set(CONNECTION.MANUAL, standardSync.getManual())
          .set(CONNECTION.SCHEDULE_TYPE,
              standardSync.getScheduleType() == null ? null
                  : Enums.toEnum(standardSync.getScheduleType().value(),
                      io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.class)
                      .orElseThrow())
          .set(CONNECTION.SCHEDULE_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getScheduleData())))
          .set(CONNECTION.RESOURCE_REQUIREMENTS,
              JSONB.valueOf(Jsons.serialize(standardSync.getResourceRequirements())))
          .set(CONNECTION.SOURCE_CATALOG_ID, standardSync.getSourceCatalogId())
          .set(CONNECTION.GEOGRAPHY, Enums.toEnum(standardSync.getGeography().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
          .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
          .set(CONNECTION.CREATED_AT, timestamp)
          .set(CONNECTION.UPDATED_AT, timestamp)
          .execute();

      updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
      updateOrCreateSchemaChangeNotificationPreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(), timestamp,
          ctx);

      for (final UUID operationIdFromStandardSync : standardSync.getOperationIds()) {
        ctx.insertInto(CONNECTION_OPERATION)
            .set(CONNECTION_OPERATION.ID, UUID.randomUUID())
            .set(CONNECTION_OPERATION.CONNECTION_ID, standardSync.getConnectionId())
            .set(CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
            .set(CONNECTION_OPERATION.CREATED_AT, timestamp)
            .set(CONNECTION_OPERATION.UPDATED_AT, timestamp)
            .execute();
      }
    }
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  private void updateOrCreateNotificationConfiguration(final StandardSync standardSync, final OffsetDateTime timestamp, final DSLContext ctx) {
    final List<NotificationConfigurationRecord> notificationConfigurations = ctx.selectFrom(NOTIFICATION_CONFIGURATION)
        .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
        .fetch();
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.webhook, standardSync, timestamp, ctx);
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.email, standardSync, timestamp, ctx);
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private void updateOrCreateSchemaChangeNotificationPreference(final UUID connectionId,
                                                                final StandardSync.NonBreakingChangesPreference nonBreakingChangesPreference,
                                                                final OffsetDateTime timestamp,
                                                                final DSLContext ctx) {
    if (nonBreakingChangesPreference == null) {
      return;
    }
    final List<SchemaManagementRecord> schemaManagementConfigurations = ctx.selectFrom(SCHEMA_MANAGEMENT)
        .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .fetch();
    if (schemaManagementConfigurations.isEmpty()) {
      ctx.insertInto(SCHEMA_MANAGEMENT)
          .set(SCHEMA_MANAGEMENT.ID, UUID.randomUUID())
          .set(SCHEMA_MANAGEMENT.CONNECTION_ID, connectionId)
          .set(SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(SCHEMA_MANAGEMENT.CREATED_AT, timestamp)
          .set(SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .execute();
    } else if (schemaManagementConfigurations.size() == 1) {
      ctx.update(SCHEMA_MANAGEMENT)
          .set(SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
          .execute();
    } else {
      throw new IllegalStateException("More than one schema management entry found for the connection: " + connectionId);
    }
  }

  /**
   * Check if an update has been made to an existing configuration and update the entry accordingly.
   * If no configuration exists, this will create an entry if the targetted notification type is being
   * enabled.
   */
  private void updateNotificationConfigurationIfNeeded(final List<NotificationConfigurationRecord> notificationConfigurations,
                                                       final NotificationType notificationType,
                                                       final StandardSync standardSync,
                                                       final OffsetDateTime timestamp,
                                                       final DSLContext ctx) {
    final Optional<NotificationConfigurationRecord> maybeConfiguration = notificationConfigurations.stream()
        .filter(notificationConfiguration -> notificationConfiguration.getNotificationType() == notificationType)
        .findFirst();

    if (maybeConfiguration.isPresent()) {
      if ((maybeConfiguration.get().getEnabled() && !standardSync.getNotifySchemaChanges())
          || (!maybeConfiguration.get().getEnabled() && standardSync.getNotifySchemaChanges())) {
        ctx.update(NOTIFICATION_CONFIGURATION)
            .set(NOTIFICATION_CONFIGURATION.ENABLED, getNotificationEnabled(standardSync, notificationType))
            .set(NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
            .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
            .and(NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE.eq(notificationType))
            .execute();
      }
    } else if (getNotificationEnabled(standardSync, notificationType)) {
      ctx.insertInto(NOTIFICATION_CONFIGURATION)
          .set(NOTIFICATION_CONFIGURATION.ID, UUID.randomUUID())
          .set(NOTIFICATION_CONFIGURATION.CONNECTION_ID, standardSync.getConnectionId())
          .set(NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE, notificationType)
          .set(NOTIFICATION_CONFIGURATION.ENABLED, true)
          .set(NOTIFICATION_CONFIGURATION.CREATED_AT, timestamp)
          .set(NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * Fetch if a notification is enabled in a standard sync based on the notification type.
   */
  @VisibleForTesting
  static boolean getNotificationEnabled(final StandardSync standardSync, final NotificationType notificationType) {
    switch (notificationType) {
      case webhook:
        return standardSync.getNotifySchemaChanges() == null ? false : standardSync.getNotifySchemaChanges();
      case email:
        return standardSync.getNotifySchemaChangesByEmail() == null ? false : standardSync.getNotifySchemaChangesByEmail();
      default:
        throw new IllegalStateException("Notification type unsupported");
    }
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

  private List<NotificationConfigurationRecord> getNotificationConfigurationByConnectionIds(final List<UUID> connectionIds) throws IOException {
    return database.query(ctx -> ctx.selectFrom(NOTIFICATION_CONFIGURATION)
        .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.in(connectionIds))
        .fetch());
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

}
