/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_TAG;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.DATAPLANE_GROUP;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.STATE;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.groupConcat;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import datadog.trace.api.Trace;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigWithMetadata;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConnectionSummary;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.Schedule;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.StreamDescriptorForDestination;
import io.airbyte.config.Tag;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.BackfillPreference;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ConnectionTagRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.TagRecord;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ConnectionServiceJooqImpl implements ConnectionService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
  public void deleteStandardSync(final UUID syncId) throws IOException {
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
  @Trace
  public StandardSync getStandardSync(final UUID connectionId)
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
  public void writeStandardSync(final StandardSync standardSync) throws IOException {
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
  public List<StandardSync> listStandardSyncsUsingOperation(final UUID operationId) throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
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
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
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
  public List<StandardSync> listWorkspaceStandardSyncs(final UUID workspaceId, final boolean includeDeleted)
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
  @Trace
  public List<StandardSync> listWorkspaceStandardSyncs(final StandardSyncQuery standardSyncQuery)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
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
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
  }

  /**
   * List connections. Paginated.
   */
  @Override
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(final List<UUID> workspaceIds,
                                                                           final List<UUID> tagIds,
                                                                           final boolean includeDeleted,
                                                                           final int pageSize,
                                                                           final int rowOffset)
      throws IOException {
    return listWorkspaceStandardSyncsPaginated(new StandardSyncsQueryPaginated(
        workspaceIds,
        tagIds,
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
                                                                           final StandardSyncsQueryPaginated standardSyncsQueryPaginated)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        // SELECT connection.* plus the connection's associated operationIds as a concatenated list
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            ACTOR.WORKSPACE_ID,
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)

        // left join with all connection_operation rows that match the connection's id.
        // left join includes connections that don't have any connection_operations
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))

        // join with source actors so that we can filter by workspaceId
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        // The schema management can be non-existent for a connection id, thus we need to do a left join
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(CONNECTION_TAG).on(CONNECTION_TAG.CONNECTION_ID.eq(CONNECTION.ID))
        .where(ACTOR.WORKSPACE_ID.in(standardSyncsQueryPaginated.workspaceIds())
            .and(standardSyncsQueryPaginated.destinationId() == null || standardSyncsQueryPaginated.destinationId().isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncsQueryPaginated.destinationId()))
            .and(standardSyncsQueryPaginated.sourceId() == null || standardSyncsQueryPaginated.sourceId().isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncsQueryPaginated.sourceId()))
            .and(standardSyncsQueryPaginated.includeDeleted() ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated))
            .and(standardSyncsQueryPaginated.tagIds() == null || standardSyncsQueryPaginated.tagIds().isEmpty()
                ? noCondition()
                : CONNECTION_TAG.TAG_ID.in(standardSyncsQueryPaginated.tagIds())))
        // group by connection.id so that the groupConcat above works
        .groupBy(CONNECTION.ID, ACTOR.WORKSPACE_ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .limit(standardSyncsQueryPaginated.pageSize())
        .offset(standardSyncsQueryPaginated.rowOffset())
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));
    return getWorkspaceIdToStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
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
  public List<StandardSync> listConnectionsBySource(final UUID sourceId, final boolean includeDeleted)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.SOURCE_ID.eq(sourceId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
  }

  /**
   * List connections that use a destination.
   *
   * @param destinationId destination id
   * @param includeDeleted include deleted
   * @return connections that use the provided destination
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listConnectionsByDestination(final UUID destinationId, final boolean includeDeleted)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.DESTINATION_ID.eq(destinationId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
  }

  /**
   * List connections for a given list of sources.
   *
   * @param sourceIds source ids
   * @param includeDeleted include deleted
   * @param includeInactive include inactive
   * @return connections that use the provided source
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listConnectionsBySources(final List<UUID> sourceIds, final boolean includeDeleted, final boolean includeInactive)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.SOURCE_ID.in(sourceIds)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated))
            .and(includeInactive ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.inactive)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
  }

  /**
   * List connections that use a destination.
   *
   * @param destinationIds destination id
   * @param includeDeleted include deleted
   * @param includeInactive include inactive
   * @return connections that use the provided destination
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSync> listConnectionsByDestinations(final List<UUID> destinationIds,
                                                          final boolean includeDeleted,
                                                          final boolean includeInactive)
      throws IOException {
    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID))
        .where(CONNECTION.DESTINATION_ID.in(destinationIds)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated))
            .and(includeInactive ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.inactive))) // Close parentheses properly here
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
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
  public List<StandardSync> listConnectionsByActorDefinitionIdAndType(final UUID actorDefinitionId,
                                                                      final String actorTypeValue,
                                                                      final boolean includeDeleted,
                                                                      final boolean includeInactive)
      throws IOException {
    final Condition actorDefinitionJoinCondition = switch (ActorType.valueOf(actorTypeValue)) {
      case source -> ACTOR.ACTOR_TYPE.eq(ActorType.source).and(ACTOR.ID.eq(CONNECTION.SOURCE_ID));
      case destination -> ACTOR.ACTOR_TYPE.eq(ActorType.destination).and(ACTOR.ID.eq(CONNECTION.DESTINATION_ID));
    };

    final Result<Record> connectionAndOperationIdsResult = database.query(ctx -> ctx
        .select(
            CONNECTION.asterisk(),
            groupConcat(CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).as(OPERATION_IDS_AGG_FIELD),
            SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
        .from(CONNECTION)
        .leftJoin(CONNECTION_OPERATION).on(CONNECTION_OPERATION.CONNECTION_ID.eq(CONNECTION.ID))
        .leftJoin(ACTOR).on(actorDefinitionJoinCondition)
        .leftJoin(SCHEMA_MANAGEMENT).on(CONNECTION.ID.eq(SCHEMA_MANAGEMENT.CONNECTION_ID))
        .where(ACTOR.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .and(includeDeleted ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.deprecated))
            .and(includeInactive ? noCondition() : CONNECTION.STATUS.notEqual(StatusType.inactive)))
        .groupBy(CONNECTION.ID, SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE))
        .fetch();

    final List<UUID> connectionIds = connectionAndOperationIdsResult.map(record -> record.get(CONNECTION.ID));

    return getStandardSyncsFromResult(connectionAndOperationIdsResult, getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds));
  }

  /**
   * List active connections that use a particular actor definition and are associated with one of the
   * given actor IDs.
   *
   * @param actorDefinitionId id of the source or destination definition.
   * @param actorTypeValue either 'source' or 'destination' enum value.
   * @param actorIds list of source or destination actor IDs to filter on.
   * @return List of connections matching the given definition and actor IDs.
   * @throws IOException in case of database access issues
   */
  @Override
  public List<ConnectionSummary> listConnectionSummaryByActorDefinitionIdAndActorIds(final UUID actorDefinitionId,
                                                                                     final String actorTypeValue,
                                                                                     final List<UUID> actorIds)
      throws IOException {
    final Condition actorJoinCondition = switch (ActorType.valueOf(actorTypeValue)) {
      case source -> ACTOR.ACTOR_TYPE.eq(ActorType.source).and(ACTOR.ID.eq(CONNECTION.SOURCE_ID));
      case destination -> ACTOR.ACTOR_TYPE.eq(ActorType.destination).and(ACTOR.ID.eq(CONNECTION.DESTINATION_ID));
    };

    final Condition actorIdFilter = ACTOR.ID.in(actorIds);

    final Result<Record5<UUID, Boolean, JSONB, UUID, UUID>> connectionSummaryResult = database.query(ctx -> ctx
        .select(CONNECTION.ID,
            CONNECTION.MANUAL,
            CONNECTION.SCHEDULE,
            CONNECTION.SOURCE_ID,
            CONNECTION.DESTINATION_ID)
        .from(CONNECTION)
        .leftJoin(ACTOR).on(actorJoinCondition)
        .where(ACTOR.ACTOR_DEFINITION_ID.eq(actorDefinitionId).and(actorIdFilter))).fetch();

    return connectionSummaryResult.map(record -> new ConnectionSummary(
        record.get(CONNECTION.ID),
        record.get(CONNECTION.MANUAL),
        Jsons.deserialize(record.get(CONNECTION.SCHEDULE).data(), Schedule.class),
        record.get(CONNECTION.SOURCE_ID),
        record.get(CONNECTION.DESTINATION_ID)));
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
  public List<StreamDescriptor> getAllStreamsForConnection(final UUID connectionId)
      throws ConfigNotFoundException, IOException {
    try {
      final StandardSync standardSync = getStandardSync(connectionId);
      return standardSync.getCatalog().getStreams().stream().map(CatalogHelpers::extractDescriptor).toList();
    } catch (final JsonValidationException e) {
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
  public ConfiguredAirbyteCatalog getConfiguredCatalogForConnection(final UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync standardSync = getStandardSync(connectionId);
    return standardSync.getCatalog();
  }

  /**
   * Get dataplane group name for a connection.
   *
   * @param connectionId connection id
   * @return dataplane group name
   * @throws IOException exception while interacting with the db
   */
  @Override
  public String getDataplaneGroupNameForConnection(final UUID connectionId) throws IOException {
    final List<String> nameString = database.query(ctx -> ctx.select(DATAPLANE_GROUP.NAME)
        .from(CONNECTION))
        .join(ACTOR).on(ACTOR.ID.eq(CONNECTION.SOURCE_ID).or(ACTOR.ID.eq(CONNECTION.DESTINATION_ID)))
        .join(WORKSPACE).on(ACTOR.WORKSPACE_ID.eq(WORKSPACE.ID))
        .join(DATAPLANE_GROUP).on(WORKSPACE.DATAPLANE_GROUP_ID.eq(DATAPLANE_GROUP.ID))
        .where(CONNECTION.ID.eq(connectionId))
        .fetchInto(String.class);

    if (nameString.isEmpty()) {
      throw new RuntimeException(String.format("Dataplane group name wasn't resolved for connectionId %s",
          connectionId));
    }
    return nameString.getFirst();
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
  public boolean getConnectionHasAlphaOrBetaConnector(final UUID connectionId) throws IOException {
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
  public Set<Long> listEarlySyncJobs(final int freeUsageInterval, final int jobsFetchRange) throws IOException {
    return database.query(ctx -> getEarlySyncJobsFromResult(ctx.fetch(
        EARLY_SYNC_JOB_QUERY, freeUsageInterval, jobsFetchRange)));
  }

  /**
   * Disable a list of connections by setting their status to inactive.
   *
   * @param connectionIds list of connection ids to disable
   * @return set of connection ids that were updated
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public Set<UUID> disableConnectionsById(final List<UUID> connectionIds) throws IOException {
    return database.transaction(ctx -> ctx.update(CONNECTION)
        .set(CONNECTION.UPDATED_AT, OffsetDateTime.now())
        .set(CONNECTION.STATUS, StatusType.inactive)
        .where(CONNECTION.ID.in(connectionIds)
            .and(CONNECTION.STATUS.eq(StatusType.active)))
        .returning(CONNECTION.ID)
        .fetchSet(CONNECTION.ID));
  }

  @Override
  public List<UUID> listConnectionIdsForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.select(CONNECTION.ID)
        .from(CONNECTION)
        .join(ACTOR).on(ACTOR.ID.eq(CONNECTION.SOURCE_ID))
        .where(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .fetchInto(UUID.class));
  }

  @Override
  public List<UUID> listConnectionIdsForOrganization(final UUID organizationId) throws IOException {
    return database.query(ctx -> ctx.select(CONNECTION.ID)
        .from(CONNECTION)
        .join(ACTOR).on(ACTOR.ID.eq(CONNECTION.SOURCE_ID))
        .join(WORKSPACE).on(WORKSPACE.ID.eq(ACTOR.WORKSPACE_ID))
        .where(WORKSPACE.ORGANIZATION_ID.eq(organizationId))
        .and(CONNECTION.STATUS.ne(StatusType.deprecated))
        .fetchInto(UUID.class));
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
   * This query retrieves billable sync jobs (jobs in a terminal status - succeeded, cancelled,
   * failed) for connections that have been created in the past 7 days OR finds the first successful
   * sync jobs for their corresponding connections. These results are used to mark these early syncs
   * as free.
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
          // Consider only jobs that are in a generally accepted terminal status
          // io/airbyte/persistence/job/models/JobStatus.java:23
          + " WHERE j.status IN ('succeeded', 'cancelled', 'failed')"
          + " AND j.config_type IN ('sync', 'refresh')"
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
          SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
          .from(CONNECTION)
          // The schema management can be non-existent for a connection id, thus we need to do a left join
          .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID));
      if (configId.isPresent()) {
        return query.where(CONNECTION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    final List<UUID> connectionIds = result.map(record -> record.get(CONNECTION.ID));
    final Map<UUID, List<TagRecord>> tagsByConnection = getTagsByConnectionIds(connectionIds);

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
          DbConverter.buildStandardSync(record, connectionOperationIds(record.get(CONNECTION.ID)), notificationConfigurationRecords,
              tagsByConnection.get(record.get(CONNECTION.ID)));
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
          .set(CONNECTION.DESTINATION_CATALOG_ID, standardSync.getDestinationCatalogId())
          .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
          .where(CONNECTION.ID.eq(standardSync.getConnectionId()))
          .execute();

      updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
      updateOrCreateSchemaChangePreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(),
          standardSync.getBackfillPreference(), timestamp,
          ctx);
      updateOrCreateConnectionTags(standardSync, ctx);

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
          .set(CONNECTION.DESTINATION_CATALOG_ID, standardSync.getDestinationCatalogId())
          .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
          .set(CONNECTION.CREATED_AT, timestamp)
          .set(CONNECTION.UPDATED_AT, timestamp)
          .execute();

      updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
      updateOrCreateSchemaChangePreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(),
          standardSync.getBackfillPreference(), timestamp,
          ctx);
      updateOrCreateConnectionTags(standardSync, ctx);

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

  private void updateOrCreateConnectionTags(final StandardSync standardSync, final DSLContext ctx) {
    if (standardSync.getTags() == null) {
      return;
    }

    final Set<UUID> newTagIds = standardSync.getTags().stream().map(Tag::getTagId).collect(Collectors.toSet());

    final Set<UUID> existingTagIds = new HashSet<>(ctx.select(CONNECTION_TAG.TAG_ID)
        .from(CONNECTION_TAG)
        .where(CONNECTION_TAG.CONNECTION_ID.eq(standardSync.getConnectionId()))
        .fetchSet(CONNECTION_TAG.TAG_ID));

    final Set<UUID> tagsToDelete = Sets.difference(existingTagIds, newTagIds);
    final Set<UUID> tagsToInsert = Sets.difference(newTagIds, existingTagIds);

    // Bulk delete any removed tags
    if (!tagsToDelete.isEmpty()) {
      ctx.deleteFrom(CONNECTION_TAG)
          .where(CONNECTION_TAG.CONNECTION_ID.eq(standardSync.getConnectionId()))
          .and(CONNECTION_TAG.TAG_ID.in(tagsToDelete))
          .execute();
    }

    // Bulk insert new tags
    if (!tagsToInsert.isEmpty()) {
      // We need to verify that the tags are associated with the workspace of the connection
      final UUID workspaceId = ctx.select(ACTOR.WORKSPACE_ID)
          .from(ACTOR)
          .where(ACTOR.ID.eq(standardSync.getSourceId()))
          .fetchOne(ACTOR.WORKSPACE_ID);

      final List<ConnectionTagRecord> records = ctx.select(TAG.ID)
          .from(TAG)
          .where(TAG.ID.in(tagsToInsert))
          .and(TAG.WORKSPACE_ID.eq(workspaceId)) // Ensure tag belongs to correct workspace
          .fetchInto(UUID.class)
          .stream()
          .map(tagId -> {
            final ConnectionTagRecord record = DSL.using(ctx.configuration()).newRecord(CONNECTION_TAG);
            record.setId(UUID.randomUUID());
            record.setConnectionId(standardSync.getConnectionId());
            record.setTagId(tagId);
            return record;
          })
          .collect(Collectors.toList());

      ctx.batchInsert(records).execute();
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
  private void updateOrCreateSchemaChangePreference(final UUID connectionId,
                                                    final StandardSync.NonBreakingChangesPreference nonBreakingChangesPreference,
                                                    final StandardSync.BackfillPreference backfillPreference,
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
          .set(SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              backfillPreference == null ? BackfillPreference.disabled : BackfillPreference.valueOf(backfillPreference.value()))
          .set(SCHEMA_MANAGEMENT.CREATED_AT, timestamp)
          .set(SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .execute();
    } else if (schemaManagementConfigurations.size() == 1) {
      ctx.update(SCHEMA_MANAGEMENT)
          .set(SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              backfillPreference == null ? BackfillPreference.disabled : BackfillPreference.valueOf(backfillPreference.value()))
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
        return standardSync.getNotifySchemaChanges() != null && standardSync.getNotifySchemaChanges();
      case email:
        return standardSync.getNotifySchemaChangesByEmail() != null && standardSync.getNotifySchemaChangesByEmail();
      default:
        throw new IllegalStateException("Notification type unsupported");
    }
  }

  @Override
  public boolean actorSyncsAnyListedStream(final UUID actorId, final List<String> streamNames) throws IOException {
    return database.query(ctx -> actorSyncsAnyListedStream(actorId, streamNames, ctx));
  }

  public static boolean actorSyncsAnyListedStream(final UUID actorId, final List<String> streamNames, final DSLContext ctx) {
    // Retrieve both active and inactive syncs to be safe - we don't know why syncs were turned off,
    // and we don't want to accidentally upgrade a sync that someone is trying to use, but was turned
    // off when they e.g. temporarily ran out of credits.
    final List<StandardSync> connectionsForActor = getNonDeprecatedConnectionsForActor(actorId, ctx);
    for (final StandardSync connection : connectionsForActor) {
      final List<String> configuredStreams =
          connection.getCatalog().getStreams().stream().map(configuredStream -> configuredStream.getStream().getName()).toList();
      if (configuredStreams.stream().anyMatch(streamNames::contains)) {
        return true;
      }
    }
    return false;
  }

  public static List<StandardSync> getNonDeprecatedConnectionsForActor(final UUID actorId, final DSLContext ctx) {
    return ctx.select(CONNECTION.asterisk())
        .from(CONNECTION)
        .where(CONNECTION.SOURCE_ID.eq(actorId).or(CONNECTION.DESTINATION_ID.eq(actorId)).and(CONNECTION.STATUS.notEqual(StatusType.deprecated)))
        .fetch().stream()
        .map(record -> record.into(CONNECTION).into(StandardSync.class))
        .collect(Collectors.toList());
  }

  private List<StandardSync> getStandardSyncsFromResult(final Result<Record> connectionAndOperationIdsResult,
                                                        final List<NotificationConfigurationRecord> allNeededNotificationConfigurations,
                                                        final Map<UUID, List<TagRecord>> tagsByConnectionId) {
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
      standardSyncs
          .add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection, tagsByConnectionId.get(connectionId)));
    }

    return standardSyncs;
  }

  private List<NotificationConfigurationRecord> getNotificationConfigurationByConnectionIds(final List<UUID> connectionIds) throws IOException {
    return database.query(ctx -> ctx.selectFrom(NOTIFICATION_CONFIGURATION)
        .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.in(connectionIds))
        .fetch());
  }

  private Map<UUID, List<TagRecord>> getTagsByConnectionIds(final List<UUID> connectionIds) throws IOException {
    final List<Record> records = database.query(ctx -> ctx.select(TAG.asterisk(), CONNECTION_TAG.CONNECTION_ID)
        .from(CONNECTION_TAG)
        .join(TAG).on(TAG.ID.eq(CONNECTION_TAG.TAG_ID))
        .where(CONNECTION_TAG.CONNECTION_ID.in(connectionIds))
        .fetch());

    final Map<UUID, List<TagRecord>> tagsByConnectionId = new HashMap<>();

    for (final UUID connectionId : connectionIds) {
      tagsByConnectionId.put(connectionId, new ArrayList<>());
    }

    for (final Record record : records) {
      final UUID connectionId = record.get(CONNECTION_TAG.CONNECTION_ID, UUID.class);
      tagsByConnectionId.putIfAbsent(connectionId, new ArrayList<>());
      tagsByConnectionId.get(connectionId).add(record.into(TagRecord.class));
    }

    return tagsByConnectionId;
  }

  @SuppressWarnings("LineLength")
  private Map<UUID, List<StandardSync>> getWorkspaceIdToStandardSyncsFromResult(final Result<Record> connectionAndOperationIdsResult,
                                                                                final List<NotificationConfigurationRecord> allNeededNotificationConfigurations,
                                                                                final Map<UUID, List<TagRecord>> tagsByConnectionId) {
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
          .add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection, tagsByConnectionId.get(connectionId)));
    }

    return workspaceIdToStandardSync;
  }

  /**
   * Get stream configuration details for all active connections using a destination.
   *
   * @param destinationId destination id
   * @return List of stream configurations containing namespace settings and stream details
   * @throws IOException if there is an issue while interacting with db
   */
  @Override
  public List<StreamDescriptorForDestination> listStreamsForDestination(final UUID destinationId, final UUID connectionId) throws IOException {
    return database.query(ctx -> {
      StringBuilder sql = new StringBuilder("""
                                                SELECT DISTINCT
                                                    c.namespace_definition,
                                                    c.namespace_format,
                                                    c.prefix,
                                                    stream_element->'stream'->>'name' AS stream_name,
                                                    stream_element->'stream'->>'namespace' AS stream_namespace,
                                                    array_agg(c.id) AS connection_ids
                                                FROM connection c,
                                                LATERAL jsonb_array_elements(c.catalog->'streams') AS stream_element
                                                WHERE c.destination_id = ?
                                                AND c.status = ?
                                            """);

      if (connectionId != null) {
        sql.append(" AND c.id != ?");
      }

      sql.append("""
                     GROUP BY
                         c.namespace_definition,
                         c.namespace_format,
                         c.prefix,
                         stream_element->'stream'->>'name',
                         stream_element->'stream'->>'namespace'
                 """);

      return ctx
          .fetch(sql.toString(),
              connectionId != null ? new Object[] {destinationId, StatusType.active, connectionId} : new Object[] {destinationId, StatusType.active})
          .map(record -> new StreamDescriptorForDestination()
              .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.fromValue(record.get("namespace_definition", String.class)))
              .withNamespaceFormat(record.get("namespace_format", String.class))
              .withStreamName(record.get("stream_name", String.class))
              .withStreamNamespace(record.get("stream_namespace", String.class))
              .withConnectionIds(Arrays.asList(record.get("connection_ids", UUID[].class)))
              .withPrefix(record.get("prefix", String.class)));
    });
  }

}
