/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Sets
import datadog.trace.api.Trace
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectionSummary
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Schedule
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.config.Tag
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.config.helpers.ScheduleHelpers
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.impls.jooq.DbConverter.buildStandardSync
import io.airbyte.data.services.shared.ConnectionJobStatus
import io.airbyte.data.services.shared.ConnectionWithJobInfo
import io.airbyte.data.services.shared.Cursor
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.BackfillPreference
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage
import io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.TagRecord
import io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Record3
import org.jooq.Record5
import org.jooq.Result
import org.jooq.SelectJoinStep
import org.jooq.SortField
import org.jooq.TableField
import org.jooq.impl.DSL
import org.jooq.impl.TableImpl
import java.io.IOException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Arrays
import java.util.Collections
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors

@Singleton
class ConnectionServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : ConnectionService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Deletes a connection (sync) and all of dependent resources (state and connection_operations).
     *
     * @param syncId - id of the sync (a.k.a. connection_id)
     * @throws IOException - error while accessing db.
     */
    @Throws(IOException::class)
    override fun deleteStandardSync(syncId: UUID) {
      database.transaction<Any?> { ctx: DSLContext ->
        deleteConfig(
          Tables.NOTIFICATION_CONFIGURATION,
          Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID,
          syncId,
          ctx,
        )
        deleteConfig(
          Tables.CONNECTION_OPERATION,
          Tables.CONNECTION_OPERATION.CONNECTION_ID,
          syncId,
          ctx,
        )
        deleteConfig(
          Tables.STATE,
          Tables.STATE.CONNECTION_ID,
          syncId,
          ctx,
        )
        deleteConfig(
          Tables.CONNECTION,
          Tables.CONNECTION.ID,
          syncId,
          ctx,
        )
        null
      }
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
    @Trace
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getStandardSync(connectionId: UUID): StandardSync {
      val result = listStandardSyncWithMetadata(Optional.of(connectionId))

      val foundMoreThanOneConfig = result.size > 1
      if (result.isEmpty()) {
        throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, connectionId.toString())
      } else {
        check(!foundMoreThanOneConfig) {
          String.format(
            "Multiple %s configs found for ID %s: %s",
            ConfigNotFoundType.STANDARD_SYNC,
            connectionId,
            result,
          )
        }
      }
      return result[0]
    }

    /**
     * Write connection.
     *
     * @param standardSync connection
     * @throws IOException - exception while interacting with the db
     */
    @Throws(IOException::class)
    override fun writeStandardSync(standardSync: StandardSync) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeStandardSync(standardSync, ctx)
        null
      }
    }

    /**
     * List connections.
     *
     * @return connections
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listStandardSyncs(): List<StandardSync> = listStandardSyncWithMetadata(Optional.empty())

    /**
     * List connections using operation.
     *
     * @param operationId operation id.
     * @return Connections that use the operation.
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listStandardSyncsUsingOperation(operationId: UUID): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx // SELECT connection.* plus the connection's associated operationIds as a concatenated list
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
              // inner join with all connection_operation rows that match the connection's id
              .from(Tables.CONNECTION)
              .join(Tables.CONNECTION_OPERATION)
              // The schema management can be non-existent for a connection id, thus we need to do a left join
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              // only keep rows for connections that have an operationId that matches the input.
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              // needs to be a sub query because we want to keep all operationIds for matching connections in the main query
              .where(
                Tables.CONNECTION.ID.`in`(
                  DSL
                    .select(Tables.CONNECTION.ID)
                    .from(Tables.CONNECTION)
                    .join(Tables.CONNECTION_OPERATION)
                    .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
                    .where(Tables.CONNECTION_OPERATION.OPERATION_ID.eq(operationId)),
                ),
              )
              // group by connection.id so that the groupConcat above works
              .groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
    }

    /**
     * List connections for workspace.
     *
     * @param workspaceId workspace id
     * @param includeDeleted include deleted
     * @return list of connections
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listWorkspaceStandardSyncs(
      workspaceId: UUID,
      includeDeleted: Boolean,
    ): List<StandardSync> = listWorkspaceStandardSyncs(StandardSyncQuery(workspaceId, null, null, includeDeleted))

    /**
     * List connections for workspace via a query.
     *
     * @param standardSyncQuery query
     * @return list of connections
     * @throws IOException if there is an issue while interacting with db.
     */
    @Trace
    @Throws(IOException::class)
    override fun listWorkspaceStandardSyncs(standardSyncQuery: StandardSyncQuery): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx // SELECT connection.* plus the connection's associated operationIds as a concatenated list
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION) // left join with all connection_operation rows that match the connection's id.
              // left join includes connections that don't have any connection_operations
              .leftJoin(Tables.CONNECTION_OPERATION)
              // The schema management can be non-existent for a connection id, thus we need to do a left join
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              // join with source actors so that we can filter by workspaceId
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .join(Tables.ACTOR)
              .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
              .where(
                Tables.ACTOR.WORKSPACE_ID
                  .eq(standardSyncQuery.workspaceId)
                  .and(
                    if (standardSyncQuery.destinationId == null || standardSyncQuery.destinationId.isEmpty()) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.DESTINATION_ID.`in`(standardSyncQuery.destinationId)
                    },
                  ).and(
                    if (standardSyncQuery.sourceId == null || standardSyncQuery.sourceId.isEmpty()) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.SOURCE_ID.`in`(standardSyncQuery.sourceId)
                    },
                  ).and(
                    if (standardSyncQuery.includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ),
              )
              // group by connection.id so that the groupConcat above works
              .groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
    }

    @Trace
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getWorkspaceStandardSyncWithJobInfo(connectionId: UUID): ConnectionWithJobInfo {
      val connectionAndOperationIdsResult =
        database.query({ ctx: DSLContext ->
          ctx
            .select(
              Tables.CONNECTION.asterisk(),
              Tables.ACTOR.NAME.`as`(SOURCE_NAME),
              Tables.ACTOR
                .`as`(DEST_ACTOR_ALIAS)
                .NAME
                .`as`(DESTINATION_NAME),
              DSL.groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).`as`(OPERATION_IDS_AGG_FIELD),
              Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
              Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              DSL
                .field(
                  DSL.name(LATEST_JOBS, STATUS),
                  JobStatus::class.java,
                ).`as`(LATEST_JOB_STATUS),
              DSL
                .field(DSL.name(LATEST_JOBS, CREATED_AT))
                .`as`(LATEST_JOB_CREATED_AT),
            ).from(Tables.CONNECTION)
            .leftJoin(Tables.CONNECTION_OPERATION)
            .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .leftJoin(Tables.SCHEMA_MANAGEMENT)
            .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .join(Tables.ACTOR)
            .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
            .join(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS))
            .on(
              Tables.CONNECTION.DESTINATION_ID.eq(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).ID),
            ).leftJoin(Tables.CONNECTION_TAG)
            .on(Tables.CONNECTION_TAG.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .leftJoin(
              DSL
                .lateral<Record3<String, JobStatus, OffsetDateTime>>(
                  ctx
                    .select(
                      JOBS.SCOPE,
                      JOBS.STATUS,
                      JOBS.CREATED_AT,
                    ).from(JOBS)
                    .where(
                      JOBS.CONFIG_TYPE
                        .eq(JobConfigType.sync)
                        .and(
                          JOBS.SCOPE.eq(
                            Tables.CONNECTION.ID.cast(
                              String::class.java,
                            ),
                          ),
                        ),
                    ).orderBy(JOBS.UPDATED_AT.desc())
                    .limit(1),
                ).asTable(LATEST_JOBS),
            ).on(DSL.trueCondition())
            // group by connection.id and sort fields so that the groupConcat above works
            .where(Tables.CONNECTION.ID.eq(connectionId))
            .groupBy(buildGroupByFields())
            .fetch()
        })

      val connectionIds =
        connectionAndOperationIdsResult.stream().map { record: Record -> record.get(Tables.CONNECTION.ID) }.collect(Collectors.toList())

      val result =
        getStandardSyncsWithJobInfoFromResult(
          connectionAndOperationIdsResult,
          getNotificationConfigurationByConnectionIds(connectionIds),
          getTagsByConnectionIds(listOf(connectionId)),
        )
      if (result.isEmpty()) {
        throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, connectionId.toString())
      }
      return result.first()
    }

    @Trace
    @Throws(IOException::class)
    override fun listWorkspaceStandardSyncsCursorPaginated(
      standardSyncQuery: StandardSyncQuery,
      workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
    ): List<ConnectionWithJobInfo> {
      val orderByFields = buildOrderByClause(workspaceResourceCursorPagination.cursor)

      val filterCondition =
        buildConnectionFilterConditions(
          standardSyncQuery,
          if (workspaceResourceCursorPagination.cursor != null) workspaceResourceCursorPagination.cursor!!.filters else null,
        )
      val cursorCondition = buildCursorCondition(workspaceResourceCursorPagination.cursor)
      val whereCondition = cursorCondition.and(filterCondition)

      val connectionAndOperationIdsResult =
        database.query({ ctx: DSLContext ->
          ctx
            .select(
              Tables.CONNECTION.asterisk(),
              Tables.ACTOR.NAME.`as`(SOURCE_NAME),
              Tables.ACTOR
                .`as`(DEST_ACTOR_ALIAS)
                .NAME
                .`as`(DESTINATION_NAME),
              DSL.groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID).separator(OPERATION_IDS_AGG_DELIMITER).`as`(OPERATION_IDS_AGG_FIELD),
              Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
              Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              DSL
                .field(
                  DSL.name(LATEST_JOBS, STATUS),
                  JobStatus::class.java,
                ).`as`(LATEST_JOB_STATUS),
              DSL
                .field(DSL.name(LATEST_JOBS, CREATED_AT))
                .`as`(LATEST_JOB_CREATED_AT),
            ).from(Tables.CONNECTION)
            .leftJoin(Tables.CONNECTION_OPERATION)
            .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .leftJoin(Tables.SCHEMA_MANAGEMENT)
            .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .join(Tables.ACTOR)
            .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
            .join(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS))
            .on(
              Tables.CONNECTION.DESTINATION_ID.eq(
                Tables.ACTOR
                  .`as`(
                    DEST_ACTOR_ALIAS,
                  ).ID,
              ),
            ).leftJoin(Tables.CONNECTION_TAG)
            .on(Tables.CONNECTION_TAG.CONNECTION_ID.eq(Tables.CONNECTION.ID))
            .leftJoin(
              DSL
                .lateral<Record3<String, JobStatus, OffsetDateTime>>(
                  ctx
                    .select(
                      JOBS.SCOPE,
                      JOBS.STATUS,
                      JOBS.CREATED_AT,
                    ).from(JOBS)
                    .where(
                      JOBS.CONFIG_TYPE
                        .eq(JobConfigType.sync)
                        .and(
                          JOBS.SCOPE.eq(
                            Tables.CONNECTION.ID.cast(
                              String::class.java,
                            ),
                          ),
                        ),
                    ).orderBy(JOBS.UPDATED_AT.desc())
                    .limit(1),
                ).asTable(LATEST_JOBS),
            ).on(DSL.trueCondition())
            // group by connection.id and sort fields so that the groupConcat above works
            .where(whereCondition)
            .groupBy(buildGroupByFields())
            .orderBy(orderByFields)
            .limit(workspaceResourceCursorPagination.pageSize)
            .fetch()
        })

      val connectionIds =
        connectionAndOperationIdsResult.stream().map { record: Record -> record.get(Tables.CONNECTION.ID) }.collect(Collectors.toList())

      return getStandardSyncsWithJobInfoFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
    }

    /**
     * Build ORDER BY clause based on sort keys.
     */
    fun buildOrderByClause(cursor: Cursor?): List<SortField<*>> {
      val orderByFields: MutableList<SortField<*>> = java.util.ArrayList()

      if (cursor == null) {
        return orderByFields
      }

      val sortKey = cursor.sortKey
      val ascending = cursor.ascending

      when (sortKey) {
        SortKey.CONNECTION_NAME ->
          orderByFields.add(
            if (ascending) {
              DSL.lower(Tables.CONNECTION.NAME).cast(String::class.java).asc()
            } else {
              DSL.lower(Tables.CONNECTION.NAME).cast(String::class.java).desc()
            },
          )
        SortKey.SOURCE_NAME ->
          orderByFields.add(
            if (ascending) {
              DSL.lower(Tables.ACTOR.NAME).cast(String::class.java).asc()
            } else {
              DSL.lower(Tables.ACTOR.NAME).cast(String::class.java).desc()
            },
          )

        SortKey.DESTINATION_NAME ->
          orderByFields.add(
            if (ascending) {
              DSL.lower(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).NAME).cast(String::class.java).asc()
            } else {
              DSL.lower(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).NAME).cast(String::class.java).desc()
            },
          )

        SortKey.LAST_SYNC -> {
          if (ascending) {
            orderByFields.add(DSL.field(DSL.name(LATEST_JOBS, CREATED_AT)).asc().nullsFirst())
          } else {
            orderByFields.add(DSL.field(DSL.name(LATEST_JOBS, CREATED_AT)).desc().nullsLast())
          }
        }

        else -> throw IllegalArgumentException("Invalid sort key for connection cursor = ${cursor.cursorId}: $sortKey")
      }

      // Always add connection ID as the final sort field for consistent pagination
      orderByFields.add(if (ascending) Tables.CONNECTION.ID.asc() else Tables.CONNECTION.ID.desc())

      return orderByFields
    }

    /**
     * Build GROUP BY fields based on sort key to ensure all ORDER BY fields are included.
     */
    fun buildGroupByFields(): List<Field<*>> {
      val groupByFields: MutableList<Field<*>> = java.util.ArrayList()

      groupByFields.add(Tables.CONNECTION.ID)
      groupByFields.add(Tables.CONNECTION.NAME)
      groupByFields.add(Tables.ACTOR.NAME)
      groupByFields.add(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).NAME)
      groupByFields.add(Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
      groupByFields.add(Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE)
      groupByFields.add(DSL.field(DSL.name(LATEST_JOBS, STATUS)))
      groupByFields.add(DSL.field(DSL.name(LATEST_JOBS, CREATED_AT)))

      return groupByFields
    }

    /**
     * Build cursor WHERE condition based on sort keys and cursor values.
     */
    fun buildCursorCondition(cursor: Cursor?): Condition {
      if (cursor == null) {
        return DSL.noCondition()
      }

      val sortKey = cursor.sortKey
      val cursorValues: MutableList<Any?> = java.util.ArrayList()
      val sortFields: MutableList<Field<*>> = java.util.ArrayList()

      when (sortKey) {
        SortKey.CONNECTION_NAME -> {
          if (cursor.connectionName != null) {
            cursorValues.add(DSL.lower(DSL.inline(cursor.connectionName)).cast(String::class.java))
            sortFields.add(DSL.lower(Tables.CONNECTION.NAME).cast(String::class.java))
          }
        }

        SortKey.SOURCE_NAME -> {
          if (cursor.sourceName != null) {
            cursorValues.add(DSL.lower(DSL.inline(cursor.sourceName)).cast(String::class.java))
            sortFields.add(DSL.lower(Tables.ACTOR.NAME).cast(String::class.java))
          }
        }

        SortKey.DESTINATION_NAME -> {
          if (cursor.destinationName != null) {
            cursorValues.add(DSL.lower(DSL.inline(cursor.destinationName)).cast(String::class.java))
            sortFields.add(DSL.lower(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).NAME).cast(String::class.java))
          }
        }

        SortKey.LAST_SYNC -> {
          if (cursor.ascending) {
            if (cursor.lastSync != null) {
              cursorValues.add(OffsetDateTime.ofInstant(Instant.ofEpochSecond(cursor.lastSync!!), ZoneOffset.UTC))
              sortFields.add(DSL.field(DSL.name(LATEST_JOBS, CREATED_AT)))
            }
          } else {
            return buildCursorConditionLastSyncDesc(cursor)
          }
        }

        else -> throw IllegalArgumentException("Invalid sort key for source cursor = ${cursor.cursorId}: $sortKey")
      }

      // Always add connection ID for consistent pagination
      if (cursor.cursorId != null) {
        cursorValues.add(cursor.cursorId)
        sortFields.add(Tables.CONNECTION.ID)
      }

      // If no cursor values, no condition needed (first page)
      if (cursorValues.isEmpty()) {
        return DSL.noCondition()
      }

      // Build row comparison for cursor pagination
      return if (cursor.ascending) {
        DSL.row(*sortFields.toTypedArray<Field<*>>()).gt(*cursorValues.toTypedArray())
      } else {
        DSL.row(*sortFields.toTypedArray<Field<*>>()).lt(*cursorValues.toTypedArray())
      }
    }

    /**
     * Build cursor condition for sorting by last sync desc.
     */
    fun buildCursorConditionLastSyncDesc(cursor: Cursor?): Condition {
      if (cursor == null || cursor.cursorId == null) {
        log.info("First page; cursor == null || cursor.connectionId == null")
        return DSL.noCondition()
      }

      val cursorId = cursor.cursorId
      val cursorLastSyncEpoch = cursor.lastSync ?: 0L
      val cursorLastSyncDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(cursorLastSyncEpoch), ZoneOffset.UTC)

      val rowLastSync = DSL.field(DSL.name(LATEST_JOBS, CREATED_AT), OffsetDateTime::class.java)
      val rowId = Tables.CONNECTION.ID

      return if (cursor.lastSync != null) {
        (rowLastSync.isNotNull.and(rowLastSync.lt(cursorLastSyncDateTime)))
          .or(rowLastSync.isNull.and(rowId.lt(cursorId)))
      } else {
        rowLastSync.isNull.and(rowId.lt(cursorId))
      }
    }

    /**
     * Builds cursor pagination based connection ID cursor. When a cursor
     * is provided, finds the connection and extracts the sort key value for pagination.
     */
    override fun buildCursorPagination(
      cursor: UUID?,
      internalSortKey: SortKey,
      filters: Filters?,
      query: StandardSyncQuery?,
      ascending: Boolean?,
      pageSize: Int?,
    ): WorkspaceResourceCursorPagination {
      if (cursor == null) {
        // No cursor - return pagination for first page with filters
        return WorkspaceResourceCursorPagination.fromValues(
          internalSortKey,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          pageSize,
          ascending,
          filters,
        )
      }

      // Cursor provided - find the connection and extract the sort key value
      val cursorConnection =
        getWorkspaceStandardSyncWithJobInfo(cursor)
          ?: throw ConfigNotFoundException("Connection", cursor.toString())

      // Extract cursor values based on sort key
      val connectionName = cursorConnection.connection().name

      val lastSync =
        cursorConnection
          .latestJobCreatedAt()
          .map { obj: OffsetDateTime -> obj.toEpochSecond() }
          .orElse(null)

      return WorkspaceResourceCursorPagination.fromValues(
        internalSortKey,
        connectionName,
        cursorConnection.sourceName(),
        null,
        cursorConnection.destinationName(),
        null,
        lastSync,
        cursor,
        pageSize,
        ascending,
        filters,
      )
    }

    /**
     * Apply state filters to the query.
     */
    fun applyStateFilters(stateFilters: List<ActorStatus>): Condition {
      var condition: Condition = DSL.falseCondition()
      for (stateFilter in stateFilters) {
        condition =
          when (stateFilter) {
            ActorStatus.ACTIVE -> condition.or(Tables.CONNECTION.STATUS.eq(StatusType.active))
            ActorStatus.INACTIVE -> condition.or(Tables.CONNECTION.STATUS.eq(StatusType.inactive))
          }
      }
      return condition
    }

    /**
     * Apply status filters to the query using the joined latest job data.
     */
    fun applyStatusFilters(statusFilters: List<ConnectionJobStatus>): Condition {
      var condition: Condition = DSL.falseCondition()
      for (statusFilter in statusFilters) {
        val statusField: Field<JobStatus> =
          DSL.field<JobStatus>(
            DSL.name(LATEST_JOBS, STATUS),
            JobStatus::class.java,
          )
        condition =
          when (statusFilter) {
            ConnectionJobStatus.FAILED ->
              condition.or(
                statusField.`in`(JobStatus.failed, JobStatus.cancelled, JobStatus.incomplete),
              )

            ConnectionJobStatus.RUNNING ->
              condition.or(
                statusField.eq(JobStatus.running),
              )

            ConnectionJobStatus.HEALTHY ->
              condition.or(
                Tables.CONNECTION.STATUS
                  .eq(StatusType.active)
                  .and(
                    statusField
                      .isNull()
                      .or(statusField.`in`(JobStatus.succeeded, JobStatus.pending)),
                  ),
              )
          }
      }
      return condition
    }

    /**
     * Build common WHERE conditions for connection queries. This ensures identical filtering logic
     * between listing and counting operations.
     */
    fun buildConnectionFilterConditions(
      standardSyncQuery: StandardSyncQuery,
      filters: Filters?,
    ): Condition {
      var condition =
        Tables.ACTOR.WORKSPACE_ID
          .eq(standardSyncQuery.workspaceId)
          .and(
            if (standardSyncQuery.destinationId.isNullOrEmpty()) {
              DSL.noCondition()
            } else {
              Tables.CONNECTION.DESTINATION_ID.`in`(standardSyncQuery.destinationId)
            },
          ).and(
            if (standardSyncQuery.sourceId.isNullOrEmpty()) {
              DSL.noCondition()
            } else {
              Tables.CONNECTION.SOURCE_ID.`in`(standardSyncQuery.sourceId)
            },
          ).and(
            if (standardSyncQuery.includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.CONNECTION.STATUS.notEqual(StatusType.deprecated)
            },
          )

      // Add dynamic filter conditions from cursor if present
      if (filters != null) {
        condition =
          condition
            // searchTerm searches across connection, source, and destination names
            .and(
              if (filters.searchTerm.isNullOrBlank()) {
                DSL.noCondition()
              } else {
                Tables.CONNECTION.NAME
                  .containsIgnoreCase(filters.searchTerm)
                  .or(Tables.ACTOR.NAME.containsIgnoreCase(filters.searchTerm))
                  .or(
                    Tables.ACTOR
                      .`as`(DEST_ACTOR_ALIAS)
                      .NAME
                      .containsIgnoreCase(filters.searchTerm),
                  )
              },
            ).and(
              if (filters.sourceDefinitionIds.isNullOrEmpty()) {
                DSL.noCondition()
              } else {
                Tables.ACTOR.ACTOR_DEFINITION_ID.`in`(filters.sourceDefinitionIds)
              },
            ).and(
              if (filters.destinationDefinitionIds.isNullOrEmpty()) {
                DSL.noCondition()
              } else {
                Tables.ACTOR
                  .`as`(DEST_ACTOR_ALIAS)
                  .ACTOR_DEFINITION_ID
                  .`in`(filters.destinationDefinitionIds)
              },
            ).and(
              if (filters.statuses.isNullOrEmpty()) {
                DSL.noCondition()
              } else {
                applyStatusFilters(filters.statuses)
              },
            ).and(
              if (filters.states.isNullOrEmpty()) {
                DSL.noCondition()
              } else {
                applyStateFilters(filters.states)
              },
            ).and(
              if (filters.tagIds.isNullOrEmpty()) {
                DSL.noCondition()
              } else {
                Tables.CONNECTION_TAG.TAG_ID.`in`(filters.tagIds)
              },
            )
      }

      return condition
    }

    /**
     * Count connections for workspace using the same filters as
     * listWorkspaceStandardSyncsCursorPaginated, including dynamic filters from cursor.
     *
     * @param standardSyncQuery query with structural filters
     * @param filters cursor pagination filters
     * @return count of connections matching the query
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun countWorkspaceStandardSyncs(
      standardSyncQuery: StandardSyncQuery,
      filters: Filters?,
    ): Int =
      database.query({ ctx: DSLContext ->
        ctx
          .selectCount()
          .from(
            ctx
              .selectDistinct(Tables.CONNECTION.ID)
              .from(Tables.CONNECTION)
              .join(Tables.ACTOR)
              .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
              .join(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS))
              .on(Tables.CONNECTION.DESTINATION_ID.eq(Tables.ACTOR.`as`(DEST_ACTOR_ALIAS).ID))
              .leftJoin(Tables.CONNECTION_TAG)
              .on(Tables.CONNECTION_TAG.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(
                DSL
                  .lateral(
                    ctx
                      .select(JOBS.SCOPE, JOBS.STATUS, JOBS.CREATED_AT)
                      .from(JOBS)
                      .where(
                        JOBS.CONFIG_TYPE
                          .eq(JobConfigType.sync)
                          .and(JOBS.SCOPE.eq(Tables.CONNECTION.ID.cast(String::class.java))),
                      ).orderBy(JOBS.UPDATED_AT.desc())
                      .limit(1),
                  ).asTable(LATEST_JOBS),
              ).on(DSL.trueCondition())
              .where(buildConnectionFilterConditions(standardSyncQuery, filters))
              .asTable("filtered_connections"),
          ).fetchSingle()
          .value1()
      })

    /**
     * List connections. Paginated.
     */
    @Throws(IOException::class)
    override fun listWorkspaceStandardSyncsLimitOffsetPaginated(
      workspaceIds: List<UUID>,
      tagIds: List<UUID>,
      includeDeleted: Boolean,
      pageSize: Int,
      rowOffset: Int,
    ): Map<UUID, MutableList<StandardSync>> =
      listWorkspaceStandardSyncsLimitOffsetPaginated(
        StandardSyncsQueryPaginated(
          workspaceIds,
          tagIds,
          null,
          null,
          includeDeleted,
          pageSize,
          rowOffset,
        ),
      )

    /**
     * List connections for workspace. Paginated.
     *
     * @param standardSyncsQueryPaginated query
     * @return Map of workspace ID -> list of connections
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listWorkspaceStandardSyncsLimitOffsetPaginated(
      standardSyncsQueryPaginated: StandardSyncsQueryPaginated,
    ): Map<UUID, MutableList<StandardSync>> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            // SELECT connection.* plus the connection's associated operationIds as a concatenated list
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.ACTOR.WORKSPACE_ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              // left join with all connection_operation rows that match the connection's id.
              // left join includes connections that don't have any connection_operations
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              // join with source actors so that we can filter by workspaceId
              .join(Tables.ACTOR)
              .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
              // The schema management can be non-existent for a connection id, thus we need to do a left join
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.CONNECTION_TAG)
              .on(Tables.CONNECTION_TAG.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .where(
                Tables.ACTOR.WORKSPACE_ID
                  .`in`(standardSyncsQueryPaginated.workspaceIds)
                  .and(
                    if (standardSyncsQueryPaginated.destinationId == null || standardSyncsQueryPaginated.destinationId.isEmpty()) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.DESTINATION_ID.`in`(standardSyncsQueryPaginated.destinationId)
                    },
                  ).and(
                    if (standardSyncsQueryPaginated.sourceId == null || standardSyncsQueryPaginated.sourceId.isEmpty()) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.SOURCE_ID.`in`(standardSyncsQueryPaginated.sourceId)
                    },
                  ).and(
                    if (standardSyncsQueryPaginated.includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ).and(
                    if (standardSyncsQueryPaginated.tagIds == null || standardSyncsQueryPaginated.tagIds.isEmpty()) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION_TAG.TAG_ID.`in`(standardSyncsQueryPaginated.tagIds)
                    },
                  ),
              )
              // group by connection.id so that the groupConcat above works
              .groupBy(
                Tables.CONNECTION.ID,
                Tables.ACTOR.WORKSPACE_ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.limit(standardSyncsQueryPaginated.pageSize)
          .offset(standardSyncsQueryPaginated.rowOffset)
          .fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }
      return getWorkspaceIdToStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
    }

    /**
     * List connections that use a source.
     *
     * @param sourceId source id
     * @param includeDeleted include deleted
     * @return connections that use the provided source
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listConnectionsBySource(
      sourceId: UUID,
      includeDeleted: Boolean,
    ): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .where(
                Tables.CONNECTION.SOURCE_ID
                  .eq(sourceId)
                  .and(
                    if (includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ),
              ).groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
    }

    /**
     * List connections that use a destination.
     *
     * @param destinationId destination id
     * @param includeDeleted include deleted
     * @return connections that use the provided destination
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listConnectionsByDestination(
      destinationId: UUID,
      includeDeleted: Boolean,
    ): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .where(
                Tables.CONNECTION.DESTINATION_ID
                  .eq(destinationId)
                  .and(
                    if (includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ),
              ).groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
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
    @Throws(IOException::class)
    override fun listConnectionsBySources(
      sourceIds: List<UUID>,
      includeDeleted: Boolean,
      includeInactive: Boolean,
    ): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .where(
                Tables.CONNECTION.SOURCE_ID
                  .`in`(sourceIds)
                  .and(
                    if (includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ).and(
                    if (includeInactive) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.inactive,
                      )
                    },
                  ),
              ).groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
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
    @Throws(IOException::class)
    override fun listConnectionsByDestinations(
      destinationIds: List<UUID>,
      includeDeleted: Boolean,
      includeInactive: Boolean,
    ): List<StandardSync> {
      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .where(
                Tables.CONNECTION.DESTINATION_ID
                  .`in`(destinationIds)
                  .and(
                    if (includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ).and(
                    if (includeInactive) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.inactive,
                      )
                    },
                  ),
              ).groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
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
    @Throws(IOException::class)
    override fun listConnectionsByActorDefinitionIdAndType(
      actorDefinitionId: UUID,
      actorTypeValue: String,
      includeDeleted: Boolean,
      includeInactive: Boolean,
    ): List<StandardSync> {
      val actorDefinitionJoinCondition =
        when (ActorType.valueOf(actorTypeValue)) {
          ActorType.source ->
            Tables.ACTOR.ACTOR_TYPE
              .eq(ActorType.source)
              .and(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          ActorType.destination ->
            Tables.ACTOR.ACTOR_TYPE
              .eq(ActorType.destination)
              .and(Tables.ACTOR.ID.eq(Tables.CONNECTION.DESTINATION_ID))
        }

      val connectionAndOperationIdsResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                DSL
                  .groupConcat(Tables.CONNECTION_OPERATION.OPERATION_ID)
                  .separator(OPERATION_IDS_AGG_DELIMITER)
                  .`as`(OPERATION_IDS_AGG_FIELD),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.CONNECTION_OPERATION)
              .on(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(Tables.CONNECTION.ID))
              .leftJoin(Tables.ACTOR)
              .on(actorDefinitionJoinCondition)
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.CONNECTION.ID.eq(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID))
              .where(
                Tables.ACTOR.ACTOR_DEFINITION_ID
                  .eq(actorDefinitionId)
                  .and(
                    if (includeDeleted) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.deprecated,
                      )
                    },
                  ).and(
                    if (includeInactive) {
                      DSL.noCondition()
                    } else {
                      Tables.CONNECTION.STATUS.notEqual(
                        StatusType.inactive,
                      )
                    },
                  ),
              ).groupBy(
                Tables.CONNECTION.ID,
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              )
          }.fetch()

      val connectionIds = connectionAndOperationIdsResult.map { record: Record -> record.get(Tables.CONNECTION.ID) }

      return getStandardSyncsFromResult(
        connectionAndOperationIdsResult,
        getNotificationConfigurationByConnectionIds(connectionIds),
        getTagsByConnectionIds(connectionIds),
      )
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
    @Throws(IOException::class)
    override fun listConnectionSummaryByActorDefinitionIdAndActorIds(
      actorDefinitionId: UUID,
      actorTypeValue: String,
      actorIds: List<UUID>,
    ): List<ConnectionSummary> {
      val actorJoinCondition =
        when (ActorType.valueOf(actorTypeValue)) {
          ActorType.source ->
            Tables.ACTOR.ACTOR_TYPE
              .eq(ActorType.source)
              .and(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          ActorType.destination ->
            Tables.ACTOR.ACTOR_TYPE
              .eq(ActorType.destination)
              .and(Tables.ACTOR.ID.eq(Tables.CONNECTION.DESTINATION_ID))
        }

      val actorIdFilter = Tables.ACTOR.ID.`in`(actorIds)

      val connectionSummaryResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(
                Tables.CONNECTION.ID,
                Tables.CONNECTION.MANUAL,
                Tables.CONNECTION.SCHEDULE,
                Tables.CONNECTION.SOURCE_ID,
                Tables.CONNECTION.DESTINATION_ID,
              ).from(Tables.CONNECTION)
              .leftJoin(Tables.ACTOR)
              .on(actorJoinCondition)
              .where(
                Tables.ACTOR.ACTOR_DEFINITION_ID
                  .eq(actorDefinitionId)
                  .and(actorIdFilter),
              )
          }.fetch()

      return connectionSummaryResult.map { record: Record5<UUID, Boolean, JSONB, UUID, UUID> ->
        ConnectionSummary(
          record.get(Tables.CONNECTION.ID),
          record.get(Tables.CONNECTION.MANUAL),
          Jsons.deserialize(
            record.get(Tables.CONNECTION.SCHEDULE).data(),
            Schedule::class.java,
          ),
          record.get(Tables.CONNECTION.SOURCE_ID),
          record.get(Tables.CONNECTION.DESTINATION_ID),
        )
      }
    }

    /**
     * Get all streams for connection.
     *
     * @param connectionId connection id
     * @return list of streams for connection
     * @throws ConfigNotFoundException if the config does not exist
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(ConfigNotFoundException::class, IOException::class)
    override fun getAllStreamsForConnection(connectionId: UUID): List<StreamDescriptor> {
      try {
        val standardSync = getStandardSync(connectionId)
        return standardSync.catalog.streams
          .stream()
          .map { airbyteStream: ConfiguredAirbyteStream -> CatalogHelpers.extractDescriptor(airbyteStream) }
          .toList()
      } catch (e: JsonValidationException) {
        throw RuntimeException(e)
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
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    override fun getConfiguredCatalogForConnection(connectionId: UUID): ConfiguredAirbyteCatalog {
      val standardSync = getStandardSync(connectionId)
      return standardSync.catalog
    }

    /**
     * Get dataplane group name for a connection.
     *
     * @param connectionId connection id
     * @return dataplane group name
     * @throws IOException exception while interacting with the db
     */
    @Throws(IOException::class)
    override fun getDataplaneGroupNameForConnection(connectionId: UUID): String {
      val nameString =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(Tables.DATAPLANE_GROUP.NAME)
              .from(Tables.CONNECTION)
          }.join(Tables.ACTOR)
          .on(
            Tables.ACTOR.ID
              .eq(Tables.CONNECTION.SOURCE_ID)
              .or(Tables.ACTOR.ID.eq(Tables.CONNECTION.DESTINATION_ID)),
          ).join(Tables.WORKSPACE)
          .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
          .join(Tables.DATAPLANE_GROUP)
          .on(Tables.WORKSPACE.DATAPLANE_GROUP_ID.eq(Tables.DATAPLANE_GROUP.ID))
          .where(Tables.CONNECTION.ID.eq(connectionId))
          .fetchInto(String::class.java)

      if (nameString.isEmpty()) {
        throw RuntimeException(
          String.format(
            "Dataplane group name wasn't resolved for connectionId %s",
            connectionId,
          ),
        )
      }
      return nameString.first()
    }

    /**
     * Specialized query for efficiently determining a connection's eligibility for the Free Connector
     * Program. If a connection has at least one Alpha or Beta connector, it will be free to use as long
     * as the workspace is enrolled in the Free Connector Program. This check is used to allow free
     * connections to continue running even when a workspace runs out of credits.
     *
     *
     * This should only be used for efficiently determining eligibility for the Free Connector Program.
     * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
     * the ReleaseStages.
     *
     * @param connectionId ID of the connection to check connectors for
     * @return boolean indicating if an alpha or beta connector is used by the connection
     */
    @Throws(IOException::class)
    override fun getConnectionHasAlphaOrBetaConnector(connectionId: UUID): Boolean {
      val releaseStageAlphaOrBeta =
        Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE
          .eq(ReleaseStage.alpha)
          .or(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.beta))

      val countResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .selectCount()
              .from(Tables.CONNECTION)
              .join(Tables.ACTOR)
              .on(
                Tables.ACTOR.ID
                  .eq(Tables.CONNECTION.SOURCE_ID)
                  .or(Tables.ACTOR.ID.eq(Tables.CONNECTION.DESTINATION_ID)),
              ).join(Tables.ACTOR_DEFINITION)
              .on(Tables.ACTOR_DEFINITION.ID.eq(Tables.ACTOR.ACTOR_DEFINITION_ID))
              .join(Tables.ACTOR_DEFINITION_VERSION)
              .on(Tables.ACTOR_DEFINITION_VERSION.ID.eq(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
              .where(Tables.CONNECTION.ID.eq(connectionId))
              .and(releaseStageAlphaOrBeta)
          }.fetchOneInto(Int::class.java)

      return countResult!! > 0
    }

    @Throws(IOException::class)
    override fun listEarlySyncJobs(
      freeUsageInterval: Int,
      jobsFetchRange: Int,
    ): Set<Long> =
      database.query { ctx: DSLContext ->
        getEarlySyncJobsFromResult(
          ctx.fetch(
            EARLY_SYNC_JOB_QUERY,
            freeUsageInterval,
            jobsFetchRange,
          ),
        )
      }

    /**
     * Disable a list of connections by setting their status to inactive.
     *
     * @param connectionIds list of connection ids to disable
     * @return set of connection ids that were updated
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun disableConnectionsById(connectionIds: List<UUID>): Set<UUID> =
      database.transaction { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTION)
          .set(
            Tables.CONNECTION.UPDATED_AT,
            OffsetDateTime.now(),
          ).set(
            Tables.CONNECTION.STATUS,
            StatusType.inactive,
          ).where(
            Tables.CONNECTION.ID
              .`in`(connectionIds)
              .and(Tables.CONNECTION.STATUS.eq(StatusType.active)),
          ).returning(Tables.CONNECTION.ID)
          .fetchSet(Tables.CONNECTION.ID)
      }

    @Throws(IOException::class)
    override fun listConnectionIdsForWorkspace(workspaceId: UUID): List<UUID> =
      database.query { ctx: DSLContext ->
        ctx
          .select(Tables.CONNECTION.ID)
          .from(Tables.CONNECTION)
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          .where(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
          .fetchInto(UUID::class.java)
      }

    @Throws(IOException::class)
    override fun listConnectionIdsForOrganization(organizationId: UUID): List<UUID> =
      database.query { ctx: DSLContext ->
        ctx
          .select(Tables.CONNECTION.ID)
          .from(Tables.CONNECTION)
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          .join(Tables.WORKSPACE)
          .on(Tables.WORKSPACE.ID.eq(Tables.ACTOR.WORKSPACE_ID))
          .where(Tables.WORKSPACE.ORGANIZATION_ID.eq(organizationId))
          .and(Tables.CONNECTION.STATUS.ne(StatusType.deprecated))
          .fetchInto(UUID::class.java)
      }

    private fun getEarlySyncJobsFromResult(result: Result<Record>): Set<Long> {
      // Transform the result to a list of early sync job ids
      // the rest of the fields are not used, we aim to keep the set small
      val earlySyncJobs: MutableSet<Long?> = HashSet()
      for (record in result) {
        earlySyncJobs.add(record["job_id"] as Long?)
      }
      return earlySyncJobs.filterNotNull().toSet()
    }

    @Throws(IOException::class)
    private fun listStandardSyncWithMetadata(configId: Optional<UUID>): List<StandardSync> {
      val result =
        database.query { ctx: DSLContext ->
          val query: SelectJoinStep<Record> =
            ctx
              .select(
                Tables.CONNECTION.asterisk(),
                Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
                Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
              ).from(Tables.CONNECTION) // The schema management can be non-existent for a connection id, thus we need to do a left join
              .leftJoin(Tables.SCHEMA_MANAGEMENT)
              .on(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(Tables.CONNECTION.ID))
          if (configId.isPresent) {
            return@query query.where(Tables.CONNECTION.ID.eq(configId.get())).fetch()
          }
          query.fetch()
        }

      val connectionIds = result.map { record: Record -> record.get(Tables.CONNECTION.ID) }
      val tagsByConnection = getTagsByConnectionIds(connectionIds)

      val standardSyncs: MutableList<StandardSync> = ArrayList()
      for (record in result) {
        val notificationConfigurationRecords: List<NotificationConfigurationRecord> =
          database.query { ctx: DSLContext ->
            if (configId.isPresent) {
              return@query ctx
                .selectFrom<NotificationConfigurationRecord>(Tables.NOTIFICATION_CONFIGURATION)
                .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(configId.get()))
                .fetch()
            } else {
              return@query ctx
                .selectFrom<NotificationConfigurationRecord>(Tables.NOTIFICATION_CONFIGURATION)
                .fetch()
            }
          }

        val standardSync =
          DbConverter.buildStandardSync(
            record,
            connectionOperationIds(record.get(Tables.CONNECTION.ID)),
            notificationConfigurationRecords,
            tagsByConnection[record.get(Tables.CONNECTION.ID)]!!,
          )
        if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
          throw RuntimeException("unexpected schedule type mismatch")
        }
        standardSyncs.add(standardSync)
      }
      return standardSyncs
    }

    @Throws(IOException::class)
    private fun connectionOperationIds(connectionId: UUID): List<UUID> {
      val result =
        database.query { ctx: DSLContext ->
          ctx
            .select(DSL.asterisk())
            .from(Tables.CONNECTION_OPERATION)
            .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
            .fetch()
        }

      val ids: MutableList<UUID> = ArrayList()
      for (record in result) {
        ids.add(record.get(Tables.CONNECTION_OPERATION.OPERATION_ID))
      }

      return ids
    }

    private fun writeStandardSync(
      standardSync: StandardSync,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.CONNECTION)
            .where(Tables.CONNECTION.ID.eq(standardSync.connectionId)),
        )

      if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
        throw RuntimeException("unexpected schedule type mismatch")
      }

      if (isExistingConfig) {
        ctx
          .update(Tables.CONNECTION)
          .set(Tables.CONNECTION.ID, standardSync.connectionId)
          .set(
            Tables.CONNECTION.NAMESPACE_DEFINITION,
            standardSync.namespaceDefinition.value().toEnum<NamespaceDefinitionType>()!!,
          ).set(Tables.CONNECTION.NAMESPACE_FORMAT, standardSync.namespaceFormat)
          .set(Tables.CONNECTION.PREFIX, standardSync.prefix)
          .set(Tables.CONNECTION.SOURCE_ID, standardSync.sourceId)
          .set(Tables.CONNECTION.DESTINATION_ID, standardSync.destinationId)
          .set(Tables.CONNECTION.NAME, standardSync.name)
          .set(Tables.CONNECTION.CATALOG, JSONB.valueOf(Jsons.serialize(standardSync.catalog)))
          .set(Tables.CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(Jsons.serialize(standardSync.fieldSelectionData)))
          .set(
            Tables.CONNECTION.STATUS,
            if (standardSync.status == null) {
              null
            } else {
              standardSync.status.value().toEnum<StatusType>()!!
            },
          ).set(Tables.CONNECTION.SCHEDULE, JSONB.valueOf(Jsons.serialize(standardSync.schedule)))
          .set(Tables.CONNECTION.MANUAL, standardSync.manual)
          .set(
            Tables.CONNECTION.SCHEDULE_TYPE,
            if (standardSync.scheduleType == null) {
              null
            } else {
              standardSync.scheduleType.value().toEnum<ScheduleType>()!!
            },
          ).set(Tables.CONNECTION.SCHEDULE_DATA, JSONB.valueOf(Jsons.serialize(standardSync.scheduleData)))
          .set(
            Tables.CONNECTION.RESOURCE_REQUIREMENTS,
            JSONB.valueOf(Jsons.serialize(standardSync.resourceRequirements)),
          ).set(Tables.CONNECTION.UPDATED_AT, timestamp)
          .set(Tables.CONNECTION.SOURCE_CATALOG_ID, standardSync.sourceCatalogId)
          .set(Tables.CONNECTION.DESTINATION_CATALOG_ID, standardSync.destinationCatalogId)
          .set(Tables.CONNECTION.BREAKING_CHANGE, standardSync.breakingChange)
          .where(Tables.CONNECTION.ID.eq(standardSync.connectionId))
          .execute()

        updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx)
        updateOrCreateSchemaChangePreference(
          standardSync.connectionId,
          standardSync.nonBreakingChangesPreference,
          standardSync.backfillPreference,
          timestamp,
          ctx,
        )
        updateOrCreateConnectionTags(standardSync, ctx)

        ctx
          .deleteFrom(Tables.CONNECTION_OPERATION)
          .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(standardSync.connectionId))
          .execute()

        for (operationIdFromStandardSync in standardSync.operationIds) {
          ctx
            .insertInto(Tables.CONNECTION_OPERATION)
            .set(Tables.CONNECTION_OPERATION.ID, UUID.randomUUID())
            .set(Tables.CONNECTION_OPERATION.CONNECTION_ID, standardSync.connectionId)
            .set(Tables.CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
            .set(Tables.CONNECTION_OPERATION.CREATED_AT, timestamp)
            .set(Tables.CONNECTION_OPERATION.UPDATED_AT, timestamp)
            .execute()
        }
      } else {
        ctx
          .insertInto(Tables.CONNECTION)
          .set(Tables.CONNECTION.ID, standardSync.connectionId)
          .set(
            Tables.CONNECTION.NAMESPACE_DEFINITION,
            standardSync.namespaceDefinition.value().toEnum<NamespaceDefinitionType>()!!,
          ).set(Tables.CONNECTION.NAMESPACE_FORMAT, standardSync.namespaceFormat)
          .set(Tables.CONNECTION.PREFIX, standardSync.prefix)
          .set(Tables.CONNECTION.SOURCE_ID, standardSync.sourceId)
          .set(Tables.CONNECTION.DESTINATION_ID, standardSync.destinationId)
          .set(Tables.CONNECTION.NAME, standardSync.name)
          .set(Tables.CONNECTION.CATALOG, JSONB.valueOf(Jsons.serialize(standardSync.catalog)))
          .set(Tables.CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(Jsons.serialize(standardSync.fieldSelectionData)))
          .set(
            Tables.CONNECTION.STATUS,
            if (standardSync.status == null) {
              null
            } else {
              standardSync.status.value().toEnum<StatusType>()!!
            },
          ).set(Tables.CONNECTION.SCHEDULE, JSONB.valueOf(Jsons.serialize(standardSync.schedule)))
          .set(Tables.CONNECTION.MANUAL, standardSync.manual)
          .set(
            Tables.CONNECTION.SCHEDULE_TYPE,
            if (standardSync.scheduleType == null) {
              null
            } else {
              standardSync.scheduleType.value().toEnum<ScheduleType>()!!
            },
          ).set(Tables.CONNECTION.SCHEDULE_DATA, JSONB.valueOf(Jsons.serialize(standardSync.scheduleData)))
          .set(
            Tables.CONNECTION.RESOURCE_REQUIREMENTS,
            JSONB.valueOf(Jsons.serialize(standardSync.resourceRequirements)),
          ).set(Tables.CONNECTION.SOURCE_CATALOG_ID, standardSync.sourceCatalogId)
          .set(Tables.CONNECTION.DESTINATION_CATALOG_ID, standardSync.destinationCatalogId)
          .set(Tables.CONNECTION.BREAKING_CHANGE, standardSync.breakingChange)
          .set(Tables.CONNECTION.CREATED_AT, timestamp)
          .set(Tables.CONNECTION.UPDATED_AT, timestamp)
          .execute()

        updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx)
        updateOrCreateSchemaChangePreference(
          standardSync.connectionId,
          standardSync.nonBreakingChangesPreference,
          standardSync.backfillPreference,
          timestamp,
          ctx,
        )
        updateOrCreateConnectionTags(standardSync, ctx)

        for (operationIdFromStandardSync in standardSync.operationIds) {
          ctx
            .insertInto(Tables.CONNECTION_OPERATION)
            .set(Tables.CONNECTION_OPERATION.ID, UUID.randomUUID())
            .set(Tables.CONNECTION_OPERATION.CONNECTION_ID, standardSync.connectionId)
            .set(Tables.CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
            .set(Tables.CONNECTION_OPERATION.CREATED_AT, timestamp)
            .set(Tables.CONNECTION_OPERATION.UPDATED_AT, timestamp)
            .execute()
        }
      }
    }

    private fun updateOrCreateConnectionTags(
      standardSync: StandardSync,
      ctx: DSLContext,
    ) {
      if (standardSync.tags == null) {
        return
      }

      val newTagIds =
        standardSync.tags
          .stream()
          .map { obj: Tag -> obj.tagId }
          .collect(Collectors.toSet())

      val existingTagIds: Set<UUID?> =
        HashSet(
          ctx
            .select(Tables.CONNECTION_TAG.TAG_ID)
            .from(Tables.CONNECTION_TAG)
            .where(Tables.CONNECTION_TAG.CONNECTION_ID.eq(standardSync.connectionId))
            .fetchSet(Tables.CONNECTION_TAG.TAG_ID),
        )

      val tagsToDelete: Set<UUID?> = Sets.difference(existingTagIds, newTagIds)
      val tagsToInsert: Set<UUID?> = Sets.difference(newTagIds, existingTagIds)

      // Bulk delete any removed tags
      if (!tagsToDelete.isEmpty()) {
        ctx
          .deleteFrom(Tables.CONNECTION_TAG)
          .where(Tables.CONNECTION_TAG.CONNECTION_ID.eq(standardSync.connectionId))
          .and(Tables.CONNECTION_TAG.TAG_ID.`in`(tagsToDelete))
          .execute()
      }

      // Bulk insert new tags
      if (!tagsToInsert.isEmpty()) {
        // We need to verify that the tags are associated with the workspace of the connection
        val workspaceId =
          ctx
            .select(Tables.ACTOR.WORKSPACE_ID)
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.ID.eq(standardSync.sourceId))
            .fetchOne(Tables.ACTOR.WORKSPACE_ID)

        val records =
          ctx
            .select(io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG.ID)
            .from(io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG)
            .where(
              io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG.ID
                .`in`(tagsToInsert),
            ).and(
              io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG.WORKSPACE_ID
                .eq(workspaceId),
            ) // Ensure tag belongs to correct workspace
            .fetchInto(UUID::class.java)
            .stream()
            .map { tagId: UUID? ->
              val record = DSL.using(ctx.configuration()).newRecord(Tables.CONNECTION_TAG)
              record.id = UUID.randomUUID()
              record.connectionId = standardSync.connectionId
              record.tagId = tagId
              record
            }.collect(Collectors.toList())

        ctx.batchInsert(records).execute()
      }
    }

    /**
     * Update the notification configuration for a give connection (StandardSync). It needs to have the
     * standard sync to be persisted before being called because one column of the configuration is a
     * foreign key on the Connection Table.
     */
    private fun updateOrCreateNotificationConfiguration(
      standardSync: StandardSync,
      timestamp: OffsetDateTime,
      ctx: DSLContext,
    ) {
      val notificationConfigurations: List<NotificationConfigurationRecord> =
        ctx
          .selectFrom(Tables.NOTIFICATION_CONFIGURATION)
          .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.connectionId))
          .fetch()
      updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.webhook, standardSync, timestamp, ctx)
      updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.email, standardSync, timestamp, ctx)
    }

    /**
     * Update the notification configuration for a give connection (StandardSync). It needs to have the
     * standard sync to be persisted before being called because one column of the configuration is a
     * foreign key on the Connection Table.
     */
    private fun updateOrCreateSchemaChangePreference(
      connectionId: UUID,
      nonBreakingChangesPreference: StandardSync.NonBreakingChangesPreference?,
      backfillPreference: StandardSync.BackfillPreference?,
      timestamp: OffsetDateTime,
      ctx: DSLContext,
    ) {
      if (nonBreakingChangesPreference == null) {
        return
      }
      val schemaManagementConfigurations: List<SchemaManagementRecord> =
        ctx
          .selectFrom(Tables.SCHEMA_MANAGEMENT)
          .where(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
          .fetch()
      if (schemaManagementConfigurations.isEmpty()) {
        ctx
          .insertInto(Tables.SCHEMA_MANAGEMENT)
          .set(Tables.SCHEMA_MANAGEMENT.ID, UUID.randomUUID())
          .set(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID, connectionId)
          .set(Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(
            Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
            if (backfillPreference == null) BackfillPreference.disabled else BackfillPreference.valueOf(backfillPreference.value()),
          ).set(Tables.SCHEMA_MANAGEMENT.CREATED_AT, timestamp)
          .set(Tables.SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .execute()
      } else if (schemaManagementConfigurations.size == 1) {
        ctx
          .update(Tables.SCHEMA_MANAGEMENT)
          .set(Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(
            Tables.SCHEMA_MANAGEMENT.BACKFILL_PREFERENCE,
            if (backfillPreference == null) BackfillPreference.disabled else BackfillPreference.valueOf(backfillPreference.value()),
          ).set(Tables.SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .where(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
          .execute()
      } else {
        throw IllegalStateException("More than one schema management entry found for the connection: $connectionId")
      }
    }

    /**
     * Check if an update has been made to an existing configuration and update the entry accordingly.
     * If no configuration exists, this will create an entry if the targetted notification type is being
     * enabled.
     */
    private fun updateNotificationConfigurationIfNeeded(
      notificationConfigurations: List<NotificationConfigurationRecord>,
      notificationType: NotificationType,
      standardSync: StandardSync,
      timestamp: OffsetDateTime,
      ctx: DSLContext,
    ) {
      val maybeConfiguration =
        notificationConfigurations
          .stream()
          .filter { notificationConfiguration: NotificationConfigurationRecord -> notificationConfiguration.notificationType == notificationType }
          .findFirst()

      if (maybeConfiguration.isPresent) {
        if ((maybeConfiguration.get().enabled && !standardSync.notifySchemaChanges) ||
          (!maybeConfiguration.get().enabled && standardSync.notifySchemaChanges)
        ) {
          ctx
            .update(Tables.NOTIFICATION_CONFIGURATION)
            .set(Tables.NOTIFICATION_CONFIGURATION.ENABLED, getNotificationEnabled(standardSync, notificationType))
            .set(Tables.NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
            .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.connectionId))
            .and(Tables.NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE.eq(notificationType))
            .execute()
        }
      } else if (getNotificationEnabled(standardSync, notificationType)) {
        ctx
          .insertInto(Tables.NOTIFICATION_CONFIGURATION)
          .set(Tables.NOTIFICATION_CONFIGURATION.ID, UUID.randomUUID())
          .set(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID, standardSync.connectionId)
          .set(Tables.NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE, notificationType)
          .set(Tables.NOTIFICATION_CONFIGURATION.ENABLED, true)
          .set(Tables.NOTIFICATION_CONFIGURATION.CREATED_AT, timestamp)
          .set(Tables.NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
          .execute()
      }
    }

    @Throws(IOException::class)
    override fun actorSyncsAnyListedStream(
      actorId: UUID,
      streamNames: List<String>,
    ): Boolean =
      database.query { ctx: DSLContext ->
        actorSyncsAnyListedStream(
          actorId,
          streamNames,
          ctx,
        )
      }

    private fun getStandardSyncsWithJobInfoFromResult(
      connectionAndOperationIdsResult: Result<Record>,
      allNeededNotificationConfigurations: List<NotificationConfigurationRecord>,
      tagsByConnectionId: Map<UUID, List<TagRecord>>,
    ): List<ConnectionWithJobInfo> {
      val connectionWithJobInfoList: MutableList<ConnectionWithJobInfo> = ArrayList()

      for (record in connectionAndOperationIdsResult) {
        val operationIdsFromRecord = record[OPERATION_IDS_AGG_FIELD, String::class.java]

        // can be null when connection has no connectionOperations
        val operationIds =
          if (operationIdsFromRecord == null) {
            Collections.emptyList()
          } else {
            Arrays
              .stream(operationIdsFromRecord.split(OPERATION_IDS_AGG_DELIMITER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
              .map { name: String? -> UUID.fromString(name) }
              .toList()
          }

        val connectionId = record.get(Tables.CONNECTION.ID)
        val notificationConfigurationsForConnection =
          allNeededNotificationConfigurations
            .stream()
            .filter { notificationConfiguration -> notificationConfiguration.connectionId.equals(connectionId) }
            .toList()

        val standardSync =
          buildStandardSync(record, operationIds, notificationConfigurationsForConnection, tagsByConnectionId[connectionId]!!)

        // Extract source & destination names
        val sourceName: String = record.get(SOURCE_NAME, String::class.java)
        val destinationName: String = record.get(DESTINATION_NAME, String::class.java)

        // Extract job data
        val latestJobStatus: JobStatus? = record.get(LATEST_JOB_STATUS, JobStatus::class.java)
        val latestJobCreatedAt: OffsetDateTime? = record.get(LATEST_JOB_CREATED_AT, OffsetDateTime::class.java)

        connectionWithJobInfoList.add(ConnectionWithJobInfo.of(standardSync, sourceName, destinationName, latestJobStatus, latestJobCreatedAt))
      }

      return connectionWithJobInfoList
    }

    private fun getStandardSyncsFromResult(
      connectionAndOperationIdsResult: Result<Record>,
      allNeededNotificationConfigurations: List<NotificationConfigurationRecord>,
      tagsByConnectionId: Map<UUID, List<TagRecord>>,
    ): List<StandardSync> {
      val standardSyncs: MutableList<StandardSync> = ArrayList()

      for (record in connectionAndOperationIdsResult) {
        val operationIdsFromRecord = record.get(OPERATION_IDS_AGG_FIELD, String::class.java)

        // can be null when connection has no connectionOperations
        val operationIds =
          if (operationIdsFromRecord == null) {
            emptyList()
          } else {
            Arrays
              .stream(operationIdsFromRecord.split(OPERATION_IDS_AGG_DELIMITER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
              .map { name: String? -> UUID.fromString(name) }
              .toList()
          }

        val connectionId = record.get(Tables.CONNECTION.ID)
        val notificationConfigurationsForConnection =
          allNeededNotificationConfigurations
            .stream()
            .filter { notificationConfiguration: NotificationConfigurationRecord -> notificationConfiguration.connectionId == connectionId }
            .toList()
        standardSyncs
          .add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection, tagsByConnectionId[connectionId]!!))
      }

      return standardSyncs
    }

    @Throws(IOException::class)
    private fun getNotificationConfigurationByConnectionIds(connectionIds: List<UUID?>): List<NotificationConfigurationRecord> =
      database.query { ctx: DSLContext ->
        ctx
          .selectFrom(Tables.NOTIFICATION_CONFIGURATION)
          .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.`in`(connectionIds))
          .fetch()
      }

    @Throws(IOException::class)
    private fun getTagsByConnectionIds(connectionIds: List<UUID>): Map<UUID, List<TagRecord>> {
      val records: List<Record> =
        database.query { ctx: DSLContext ->
          ctx
            .select(
              io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG
                .asterisk(),
              Tables.CONNECTION_TAG.CONNECTION_ID,
            ).from(Tables.CONNECTION_TAG)
            .join(io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG)
            .on(
              io.airbyte.db.instance.configs.jooq.generated.tables.Tag.TAG.ID
                .eq(Tables.CONNECTION_TAG.TAG_ID),
            ).where(Tables.CONNECTION_TAG.CONNECTION_ID.`in`(connectionIds))
            .fetch()
        }

      val tagsByConnectionId: MutableMap<UUID, MutableList<TagRecord>> = HashMap()

      for (connectionId in connectionIds) {
        tagsByConnectionId[connectionId] = ArrayList()
      }

      for (record in records) {
        val connectionId =
          record.get(
            Tables.CONNECTION_TAG.CONNECTION_ID,
            UUID::class.java,
          )
        tagsByConnectionId.putIfAbsent(connectionId, ArrayList())
        tagsByConnectionId[connectionId]!!.add(record.into(TagRecord::class.java))
      }

      return tagsByConnectionId
    }

    private fun getWorkspaceIdToStandardSyncsFromResult(
      connectionAndOperationIdsResult: Result<Record>,
      allNeededNotificationConfigurations: List<NotificationConfigurationRecord>,
      tagsByConnectionId: Map<UUID, List<TagRecord>>,
    ): Map<UUID, MutableList<StandardSync>> {
      val workspaceIdToStandardSync: MutableMap<UUID, MutableList<StandardSync>> = HashMap()

      for (record in connectionAndOperationIdsResult) {
        val operationIdsFromRecord = record.get(OPERATION_IDS_AGG_FIELD, String::class.java)

        // can be null when connection has no connectionOperations
        val operationIds =
          if (operationIdsFromRecord == null) {
            emptyList()
          } else {
            Arrays
              .stream(operationIdsFromRecord.split(OPERATION_IDS_AGG_DELIMITER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
              .map { name: String? -> UUID.fromString(name) }
              .toList()
          }

        val connectionId = record.get(Tables.CONNECTION.ID)
        val notificationConfigurationsForConnection =
          allNeededNotificationConfigurations
            .stream()
            .filter { notificationConfiguration: NotificationConfigurationRecord -> notificationConfiguration.connectionId == connectionId }
            .toList()
        workspaceIdToStandardSync
          .computeIfAbsent(
            record.get(Tables.ACTOR.WORKSPACE_ID),
          ) { v: UUID? -> ArrayList() }
          .add(DbConverter.buildStandardSync(record, operationIds, notificationConfigurationsForConnection, tagsByConnectionId[connectionId]!!))
      }

      return workspaceIdToStandardSync
    }

    /**
     * Get stream configuration details for all active connections using a destination.
     *
     * @param destinationId destination id
     * @return List of stream configurations containing namespace settings and stream details
     * @throws IOException if there is an issue while interacting with db
     */
    @Throws(IOException::class)
    override fun listStreamsForDestination(
      destinationId: UUID,
      connectionId: UUID?,
    ): List<StreamDescriptorForDestination> =
      database.query { ctx: DSLContext ->
        val sql =
          StringBuilder(
            """
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
            
            """.trimIndent(),
          )
        if (connectionId != null) {
          sql.append(" AND c.id != ? ")
        }

        sql.append(
          """
            
          GROUP BY
              c.namespace_definition,
              c.namespace_format,
              c.prefix,
              stream_element->'stream'->>'name',
              stream_element->'stream'->>'namespace'
          
          """.trimIndent(),
        )
        ctx
          .fetch(
            sql.toString(),
            *if (connectionId != null) {
              arrayOf<Any>(
                destinationId,
                StatusType.active,
                connectionId,
              )
            } else {
              arrayOf<Any>(destinationId, StatusType.active)
            },
          ).map { record: Record ->
            StreamDescriptorForDestination()
              .withNamespaceDefinition(
                JobSyncConfig.NamespaceDefinitionType.fromValue(
                  record.get(
                    "namespace_definition",
                    String::class.java,
                  ),
                ),
              ).withNamespaceFormat(record.get("namespace_format", String::class.java))
              .withStreamName(record.get("stream_name", String::class.java))
              .withStreamNamespace(record.get("stream_namespace", String::class.java))
              .withConnectionIds(
                Arrays.asList(
                  *record.get(
                    "connection_ids",
                    Array<UUID>::class.java,
                  ),
                ),
              ).withPrefix(record.get("prefix", String::class.java))
          }
      }

    companion object {
      private val log = KotlinLogging.logger {}

      private const val OPERATION_IDS_AGG_DELIMITER = ","
      private const val OPERATION_IDS_AGG_FIELD = "operation_ids_agg"
      private const val CREATED_AT: String = "created_at"
      private const val DEST_ACTOR_ALIAS: String = "dest_actor"
      private const val LATEST_JOBS: String = "latest_jobs"
      private const val LATEST_JOB_STATUS: String = "latest_job_status"
      private const val LATEST_JOB_CREATED_AT: String = "latest_job_created_at"
      private const val STATUS: String = "status"
      private const val SOURCE_NAME: String = "source_name"
      private const val DESTINATION_NAME: String = "destination_name"

      /**
       * This query retrieves billable sync jobs (jobs in a terminal status - succeeded, cancelled,
       * failed) for connections that have been created in the past 7 days OR finds the first successful
       * sync jobs for their corresponding connections. These results are used to mark these early syncs
       * as free.
       */
      private const val EARLY_SYNC_JOB_QUERY = // Find the first successful sync job ID for every connection.
        // This will be used in a join below to check if a particular job is the connection's
        // first successful sync
        (
          "WITH FirstSuccessfulJobIdByConnection AS (" +
            " SELECT j2.scope, MIN(j2.id) AS min_job_id" +
            " FROM jobs j2" +
            " WHERE j2.status = 'succeeded' AND j2.config_type = 'sync'" +
            " GROUP BY j2.scope" +
            ")" +
            // Left join Jobs on Connection and the above MinJobIds, and only keep billable
            // sync jobs that have an associated Connection ID
            " SELECT j.id AS job_id, j.created_at, c.id AS conn_id, c.created_at AS connection_created_at, min_job_id" +
            " FROM jobs j" +
            " LEFT JOIN connection c ON c.id = UUID(j.scope)" +
            " LEFT JOIN FirstSuccessfulJobIdByConnection min_j_ids ON j.id = min_j_ids.min_job_id" +
            // Consider only jobs that are in a generally accepted terminal status
            // io/airbyte/persistence/job/models/JobStatus.java:23
            " WHERE j.status IN ('succeeded', 'cancelled', 'failed')" +
            " AND j.config_type IN ('sync', 'refresh')" +
            " AND c.id IS NOT NULL" +
            // Keep a job if it was created within 7 days of its connection's creation,
            // OR if it was the first successful sync job of its connection
            " AND ((j.created_at < c.created_at + make_interval(days => ?))" +
            "      OR min_job_id IS NOT NULL)" +
            // Only consider jobs that were created in the last 30 days, to cut down the query size.
            " AND j.created_at > now() - make_interval(days => ?);"
        )

      /**
       * Helper to delete records from the database.
       *
       * @param table the table to delete from
       * @param keyColumn the column to use as a key
       * @param configId the id of the object to delete, must be from the keyColumn
       * @param ctx the db context to use
       */
      private fun <T : Record?> deleteConfig(
        table: TableImpl<T>,
        keyColumn: TableField<T, UUID>,
        configId: UUID,
        ctx: DSLContext,
      ) {
        val isExistingConfig =
          ctx.fetchExists(
            DSL
              .select()
              .from(table)
              .where(keyColumn.eq(configId)),
          )

        if (isExistingConfig) {
          ctx
            .deleteFrom(table)
            .where(keyColumn.eq(configId))
            .execute()
        }
      }

      /**
       * Fetch if a notification is enabled in a standard sync based on the notification type.
       */
      @VisibleForTesting
      fun getNotificationEnabled(
        standardSync: StandardSync,
        notificationType: NotificationType,
      ): Boolean =
        when (notificationType) {
          NotificationType.webhook -> standardSync.notifySchemaChanges != null && standardSync.notifySchemaChanges
          NotificationType.email -> standardSync.notifySchemaChangesByEmail != null && standardSync.notifySchemaChangesByEmail
          else -> throw IllegalStateException("Notification type unsupported")
        }

      fun actorSyncsAnyListedStream(
        actorId: UUID?,
        streamNames: List<String>,
        ctx: DSLContext,
      ): Boolean {
        // Retrieve both active and inactive syncs to be safe - we don't know why syncs were turned off,
        // and we don't want to accidentally upgrade a sync that someone is trying to use, but was turned
        // off when they e.g. temporarily ran out of credits.
        val connectionsForActor = getNonDeprecatedConnectionsForActor(actorId, ctx)
        for (connection in connectionsForActor) {
          val configuredStreams =
            connection.catalog.streams
              .stream()
              .map { configuredStream: ConfiguredAirbyteStream -> configuredStream.stream.name }
              .toList()
          if (configuredStreams.stream().anyMatch { o: String -> streamNames.contains(o) }) {
            return true
          }
        }
        return false
      }

      fun getNonDeprecatedConnectionsForActor(
        actorId: UUID?,
        ctx: DSLContext,
      ): List<StandardSync> =
        ctx
          .select(Tables.CONNECTION.asterisk())
          .from(Tables.CONNECTION)
          .where(
            Tables.CONNECTION.SOURCE_ID.eq(actorId).or(Tables.CONNECTION.DESTINATION_ID.eq(actorId)).and(
              Tables.CONNECTION.STATUS.notEqual(
                StatusType.deprecated,
              ),
            ),
          ).fetch()
          .stream()
          .map { record: Record ->
            record.into(Tables.CONNECTION).into(
              StandardSync::class.java,
            )
          }.collect(Collectors.toList())
    }

    @Throws(IOException::class)
    override fun getConnectionStatusCounts(workspaceId: UUID): ConnectionService.ConnectionStatusCounts {
      val sql =
        """
        SELECT
          COALESCE(SUM(CASE WHEN c.status != 'inactive' AND lj.latest_status = 'running' THEN 1 ELSE 0 END), 0) AS running,
          COALESCE(SUM(CASE WHEN c.status != 'inactive' AND lj.latest_status = 'succeeded' THEN 1 ELSE 0 END), 0) AS healthy,
          COALESCE(SUM(CASE WHEN c.status != 'inactive' AND lj.latest_status IN ('failed', 'cancelled', 'incomplete') THEN 1 ELSE 0 END), 0) AS failed,
          COALESCE(SUM(CASE WHEN c.status = 'inactive' THEN 1 ELSE 0 END), 0) AS paused,
          COALESCE(SUM(CASE WHEN c.status != 'inactive' AND lj.latest_status IS NULL THEN 1 ELSE 0 END), 0) AS not_synced
        FROM connection c
        JOIN actor a ON c.source_id = a.id
        LEFT JOIN LATERAL (
          SELECT j.status AS latest_status, scope as connection_id
          FROM jobs j
          WHERE j.config_type = 'sync'
            AND j.scope = c.id::text
          ORDER BY j.created_at DESC
          LIMIT 1
        ) lj on lj.connection_id = c.id::text
        WHERE a.workspace_id = ?
          AND c.status != 'deprecated'
        
        """.trimIndent()

      return database.query { ctx: DSLContext ->
        val result = ctx.fetch(sql, workspaceId)[0]
        ConnectionService.ConnectionStatusCounts(
          result.get("running", Int::class.java),
          result.get("healthy", Int::class.java),
          result.get("failed", Int::class.java),
          result.get("paused", Int::class.java),
          result.get("not_synced", Int::class.java),
        )
      }
    }
  }
