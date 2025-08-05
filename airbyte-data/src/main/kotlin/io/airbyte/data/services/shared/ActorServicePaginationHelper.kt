/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import datadog.trace.api.Trace
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.JobStatus
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.impls.jooq.DbConverter
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.io.IOException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private val logger = KotlinLogging.logger { }

@Singleton
class ActorServicePaginationHelper(
  @Named("configDatabase") database: Database,
) {
  private val database: ExceptionWrappingDatabase

  init {
    this.database = ExceptionWrappingDatabase(database)
  }

  fun listWorkspaceActorConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
    actorType: ActorType,
  ): List<ActorConnectionWithCount> =
    database.query { ctx: DSLContext ->
      val filters = workspaceResourceCursorPagination.cursor?.filters
      val filterClause = buildActorFilterCondition(workspaceId, filters, actorType, useAliasActorDefinitionName = true)
      val cursorClause = buildCursorCondition(workspaceResourceCursorPagination.cursor, actorType)
      val (whereClause, params) = buildCombinedWhereClause(filterClause, cursorClause)
      val orderByClause = buildOrderByClause(workspaceResourceCursorPagination.cursor, actorType)

      val connectionCount = DSL.field("connection_count", Long::class.java)
      val lastSync = DSL.field("last_sync", OffsetDateTime::class.java)
      val actorDefinitionName = DSL.field("actor_definition_name", String::class.java)
      val succeededCount = DSL.field("succeeded_count", Long::class.java)
      val failedCount = DSL.field("failed_count", Long::class.java)
      val runningCount = DSL.field("running_count", Long::class.java)
      val pendingCount = DSL.field("pending_count", Long::class.java)
      val incompleteCount = DSL.field("incomplete_count", Long::class.java)
      val cancelledCount = DSL.field("cancelled_count", Long::class.java)
      val isActive = DSL.field("is_active", Boolean::class.java)

      val actorTypeStr = actorType.literal
      val connectionField = getConnectionField(actorType)
      val actorIdAlias = getConnectionField(actorType)

      val sql =
        """
        WITH cte AS (
          SELECT 
            a.*,
            ad.name as actor_definition_name
          FROM actor a
          LEFT JOIN actor_definition ad ON ad.id = a.actor_definition_id
          WHERE a.tombstone != true
            AND a.actor_type = '$actorTypeStr'
            AND a.workspace_id = '$workspaceId'
        ),
        connection_stats AS (
          SELECT 
            a.id AS $actorIdAlias,
            COUNT(c.id) FILTER (WHERE c.id IS NOT NULL) AS connection_count,
            MAX(lj.job_created_at) AS last_sync,
            COUNT(CASE WHEN lj.job_status = 'succeeded' THEN 1 END) AS succeeded_count,
            COUNT(CASE WHEN lj.job_status = 'failed' THEN 1 END) AS failed_count,
            COUNT(CASE WHEN lj.job_status = 'running' THEN 1 END) AS running_count,
            COUNT(CASE WHEN lj.job_status = 'pending' THEN 1 END) AS pending_count,
            COUNT(CASE WHEN lj.job_status = 'incomplete' THEN 1 END) AS incomplete_count,
            COUNT(CASE WHEN lj.job_status = 'cancelled' THEN 1 END) AS cancelled_count,
            CASE 
              WHEN EXISTS (
                SELECT 1
                FROM connection cx
                WHERE cx.$connectionField IS NOT NULL
                  AND cx.$connectionField = a.id
                  AND cx.status = 'active'
              ) THEN true 
              ELSE false 
            END AS is_active          FROM cte a
          LEFT JOIN connection c ON c.$connectionField = a.id AND c.status != 'deprecated'
          LEFT JOIN LATERAL (
            SELECT 
              j.created_at AS job_created_at, 
              j.status AS job_status
            FROM jobs j
            WHERE j.config_type = 'sync'
              AND j.scope = c.id::text
            ORDER BY j.created_at DESC
            LIMIT 1
          ) lj ON c.id IS NOT NULL
          GROUP BY a.id
        )
        SELECT 
          a.*,
          cs.connection_count,
          cs.last_sync,
          cs.succeeded_count,
          cs.failed_count,
          cs.running_count,
          cs.pending_count,
          cs.incomplete_count,
          cs.cancelled_count,
          cs.is_active
        FROM cte a
        LEFT JOIN connection_stats cs ON cs.$actorIdAlias = a.id
        $whereClause
        $orderByClause
        LIMIT ?
        """.trimIndent()

      val bindParams = params + workspaceResourceCursorPagination.pageSize

      logger.debug { "Executing SQL: \n$sql" }

      val result = ctx.fetch(sql, *bindParams.toTypedArray())

      result.map { record ->
        val count = (record.get(connectionCount) ?: 0L).toInt()
        val lastSyncTime = record.get(lastSync)

        val statusCounts = mutableMapOf<JobStatus, Int>()
        statusCounts[JobStatus.SUCCEEDED] = record.get(succeededCount).toInt()
        statusCounts[JobStatus.FAILED] = record.get(failedCount).toInt()
        statusCounts[JobStatus.RUNNING] = record.get(runningCount).toInt()
        statusCounts[JobStatus.PENDING] = record.get(pendingCount).toInt()
        statusCounts[JobStatus.INCOMPLETE] = record.get(incompleteCount).toInt()
        statusCounts[JobStatus.CANCELLED] = record.get(cancelledCount).toInt()

        if (actorType == ActorType.source) {
          ActorConnectionWithCount.fromSource(
            SourceConnectionWithCount(
              DbConverter.buildSourceConnection(record),
              record.get(actorDefinitionName),
              count,
              lastSyncTime,
              statusCounts,
              record.get(isActive),
            ),
          )
        } else {
          ActorConnectionWithCount.fromDestination(
            DestinationConnectionWithCount(
              DbConverter.buildDestinationConnection(record),
              record.get(actorDefinitionName),
              count,
              lastSyncTime,
              statusCounts,
              record.get(isActive),
            ),
          )
        }
      }
    }

  @Throws(IOException::class)
  fun countWorkspaceActorsFiltered(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
    actorType: ActorType,
  ): Int =
    database.query { ctx: DSLContext ->
      val filters = workspaceResourceCursorPagination.cursor?.filters
      val (whereClause, params) = buildActorFilterCondition(workspaceId, filters, actorType, useAliasActorDefinitionName = false)

      val sql =
        """
        SELECT COUNT(DISTINCT a.id) AS actor_count
        FROM actor a
        LEFT JOIN actor_definition ad ON ad.id = a.actor_definition_id
        $whereClause
          AND a.tombstone != true
          AND a.actor_type = '${actorType.literal}'
        """.trimIndent()

      ctx.fetchOne(sql, *params.toTypedArray())?.get("actor_count", Int::class.java) ?: 0
    }

  fun buildCombinedWhereClause(
    filter: Pair<String, List<Any?>>,
    cursor: Pair<String, List<Any?>>,
  ): Pair<String, List<Any?>> {
    val (filterClause, filterParams) = filter
    val (cursorClause, cursorParams) = cursor

    val combinedClause =
      when {
        filterClause.isBlank() && cursorClause.isBlank() -> ""
        filterClause.isNotBlank() && cursorClause.isBlank() -> filterClause
        filterClause.isBlank() && cursorClause.isNotBlank() ->
          "WHERE " + cursorClause.removePrefix("AND").trim()
        else -> {
          val base = filterClause.removePrefix("WHERE").trim()
          "WHERE $base ${cursorClause.trimStart()}"
        }
      }

    return combinedClause to filterParams + cursorParams
  }

  fun buildActorFilterCondition(
    workspaceId: UUID,
    filters: Filters?,
    actorType: ActorType,
    useAliasActorDefinitionName: Boolean,
  ): Pair<String, List<Any?>> {
    val conditions = mutableListOf<String>()
    val params = mutableListOf<Any?>()
    val connectionField = getConnectionField(actorType)
    val actorDefinitionField = if (useAliasActorDefinitionName) "a.actor_definition_name" else "ad.name"

    conditions += "a.workspace_id = ?"
    params += workspaceId

    if (!filters?.searchTerm.isNullOrBlank()) {
      conditions +=
        """
        (
          a.name ILIKE ? OR
          $actorDefinitionField ILIKE ?
        )
        """.trimIndent()
      params += "%${filters?.searchTerm}%"
      params += "%${filters?.searchTerm}%"
    }

    if (!filters?.states.isNullOrEmpty()) {
      val activeStates = filters?.states?.filter { it == ActorStatus.ACTIVE }
      val inactiveStates = filters?.states?.filter { it == ActorStatus.INACTIVE }

      when {
        activeStates!!.isNotEmpty() && inactiveStates!!.isNotEmpty() -> {
          // no filter
        }
        activeStates.isNotEmpty() -> {
          conditions +=
            """
            EXISTS (
              SELECT 1 
              FROM connection c 
              WHERE c.$connectionField = a.id 
                AND c.status = 'active'
                AND c.status != 'deprecated'
            )
            """.trimIndent()
        }
        inactiveStates!!.isNotEmpty() -> {
          conditions +=
            """
            NOT EXISTS (
              SELECT 1 
              FROM connection c 
              WHERE c.$connectionField = a.id 
                AND c.status = 'active'
                AND c.status != 'deprecated'
            )
            """.trimIndent()
        }
      }
    }

    val clause = "WHERE " + conditions.joinToString("\n  AND ")
    return clause to params
  }

  fun buildCursorCondition(
    cursor: Cursor?,
    actorType: ActorType,
  ): Pair<String, List<Any?>> {
    if (cursor == null) return "" to emptyList()

    val conditions = mutableListOf<String>()
    val params = mutableListOf<Any?>()

    when (cursor.sortKey) {
      SortKey.SOURCE_NAME -> {
        if (actorType == ActorType.source && !cursor.sourceName.isNullOrBlank() && cursor.cursorId != null) {
          conditions += "(LOWER(a.name), a.id) ${if (cursor.ascending) ">" else "<"} (LOWER(?), ?)"
          params += cursor.sourceName
          params += cursor.cursorId
        }
      }

      SortKey.DESTINATION_NAME -> {
        if (actorType == ActorType.destination && !cursor.destinationName.isNullOrBlank() && cursor.cursorId != null) {
          conditions += "(LOWER(a.name), a.id) ${if (cursor.ascending) ">" else "<"} (LOWER(?), ?)"
          params += cursor.destinationName
          params += cursor.cursorId
        }
      }

      SortKey.SOURCE_DEFINITION_NAME -> {
        if (actorType == ActorType.source && !cursor.sourceDefinitionName.isNullOrBlank() && cursor.cursorId != null) {
          conditions += "(LOWER(a.actor_definition_name), a.id) ${if (cursor.ascending) ">" else "<"} (LOWER(?), ?)"
          params += cursor.sourceDefinitionName
          params += cursor.cursorId
        }
      }

      SortKey.DESTINATION_DEFINITION_NAME -> {
        if (actorType == ActorType.destination && !cursor.destinationDefinitionName.isNullOrBlank() && cursor.cursorId != null) {
          conditions += "(LOWER(a.actor_definition_name), a.id) ${if (cursor.ascending) ">" else "<"} (LOWER(?), ?)"
          params += cursor.destinationDefinitionName
          params += cursor.cursorId
        }
      }

      SortKey.LAST_SYNC -> {
        if (cursor.cursorId == null) {
          // No cursor provided, return no condition
          return "" to emptyList()
        }

        val op = if (cursor.ascending) ">" else "<"
        val row = "cs.last_sync"
        val idClause = "a.id $op ?"

        if (cursor.lastSync != null) {
          val timestampClause = "$row $op CAST(? AS TIMESTAMP WITH TIME ZONE)"
          val nullCheckClause = "$row IS NULL AND $idClause"
          conditions += "(($timestampClause) OR ($nullCheckClause))"
          val ts = OffsetDateTime.ofInstant(Instant.ofEpochSecond(cursor.lastSync!!), ZoneOffset.UTC)
          params += ts
          params += cursor.cursorId
        } else {
          conditions += "$row IS NULL AND $idClause"
          params += cursor.cursorId
        }

        return "AND (${conditions.joinToString(" AND ")})" to params
      }

      else -> throw IllegalArgumentException("Invalid sort key: ${cursor.sortKey}")
    }

    return if (conditions.isEmpty()) {
      "" to emptyList()
    } else {
      "AND (${conditions.joinToString(" AND ")})" to params
    }
  }

  fun buildCursorConditionLastSyncDesc(
    cursor: Cursor?,
    actorType: ActorType,
  ): String {
    if (cursor == null || cursor.cursorId == null) {
      return ""
    }

    return if (cursor.lastSync != null) {
      """
      AND (
        (cs.last_sync IS NOT NULL AND cs.last_sync < :lastSync)
        OR (cs.last_sync IS NULL AND a.id < :cursorId)
      )
      """.trimIndent()
    } else {
      "AND cs.last_sync IS NULL AND a.id < :cursorId"
    }
  }

  /**
   * Build ORDER BY clause based on sort keys.
   */
  fun buildOrderByClause(
    cursor: Cursor?,
    actorType: ActorType,
  ): String {
    if (cursor == null) {
      return ""
    }

    val sortKey = cursor.sortKey
    val ascending = cursor.ascending
    val direction = if (ascending) "ASC" else "DESC"

    val field =
      when (sortKey) {
        SortKey.SOURCE_NAME ->
          if (actorType == ActorType.source) {
            "LOWER(a.name)"
          } else {
            throw IllegalArgumentException("SOURCE_NAME sort key is only valid for source actors")
          }
        SortKey.DESTINATION_NAME ->
          if (actorType == ActorType.destination) {
            "LOWER(a.name)"
          } else {
            throw IllegalArgumentException("DESTINATION_NAME sort key is only valid for destination actors")
          }
        SortKey.SOURCE_DEFINITION_NAME ->
          if (actorType == ActorType.source) {
            "LOWER(a.actor_definition_name)"
          } else {
            throw IllegalArgumentException("SOURCE_DEFINITION_NAME sort key is only valid for source actors")
          }
        SortKey.DESTINATION_DEFINITION_NAME ->
          if (actorType == ActorType.destination) {
            "LOWER(a.actor_definition_name)"
          } else {
            throw IllegalArgumentException("DESTINATION_DEFINITION_NAME sort key is only valid for destination actors")
          }
        SortKey.LAST_SYNC -> "cs.last_sync"
        else -> throw IllegalArgumentException("Invalid sort key for connection cursor = ${cursor.cursorId}: $sortKey")
      }

    val nulls =
      when (sortKey) {
        SortKey.LAST_SYNC ->
          if (ascending) "NULLS FIRST" else "NULLS LAST"
        else -> ""
      }

    // Always add actor ID for tie-breaking pagination
    return "ORDER BY $field $direction $nulls, a.id $direction"
  }

  /**
   * Builds cursor pagination based actor ID cursor. When a cursor
   * is provided, finds the actor and extracts the sort key value for pagination.
   */
  fun buildCursorPagination(
    cursorId: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    ascending: Boolean?,
    pageSize: Int?,
    actorType: ActorType,
  ): WorkspaceResourceCursorPagination {
    if (cursorId == null) {
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

    // Cursor provided - find the actor and extract the sort key value
    return getCursorActor(cursorId, internalSortKey, filters, ascending, pageSize, actorType)
  }

  @Trace
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getCursorActor(
    cursorId: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    ascending: Boolean?,
    pageSize: Int?,
    actorType: ActorType,
  ): WorkspaceResourceCursorPagination {
    val connectionField = getConnectionField(actorType)
    val configNotFoundType = if (actorType == ActorType.source) ConfigNotFoundType.SOURCE_CONNECTION else ConfigNotFoundType.DESTINATION_CONNECTION

    val results =
      database.query { ctx: DSLContext ->
        val sql =
          """
          WITH workspace_connections AS (
            SELECT 
              a.id,
              a.name,
              ad.name AS actor_definition_name,
              c.id AS connection_id,
              c.status as connection_status
            FROM actor a
            LEFT JOIN connection c ON c.$connectionField = a.id
            LEFT JOIN actor_definition ad ON ad.id = a.actor_definition_id
            WHERE a.id = ?
              AND a.tombstone = false
              AND a.actor_type = '${actorType.literal}'
          )
          SELECT 
            wc.id,
            wc.name,
            wc.actor_definition_name,
            lj.job_created_at AS last_sync
          FROM workspace_connections wc
          LEFT JOIN LATERAL (
            SELECT j.created_at AS job_created_at
            FROM jobs j
            WHERE j.config_type = 'sync'
              AND j.scope = wc.connection_id::text
              AND (wc.connection_status IS NOT NULL AND wc.connection_status != 'deprecated')
            ORDER BY j.created_at DESC
            LIMIT 1
          ) lj ON TRUE;
          """.trimIndent()

        ctx.fetch(sql, cursorId).map { record ->
          WorkspaceResourceCursorPagination.fromValues(
            internalSortKey,
            null,
            if (actorType == ActorType.source) record.get("name", String::class.java) else null,
            if (actorType == ActorType.source) record.get("actor_definition_name", String::class.java) else null,
            if (actorType == ActorType.destination) record.get("name", String::class.java) else null,
            if (actorType == ActorType.destination) record.get("actor_definition_name", String::class.java) else null,
            if (record.get("last_sync") != null) record.get("last_sync", OffsetDateTime::class.java).toEpochSecond() else null,
            cursorId,
            pageSize,
            ascending,
            filters,
          )
        }
      }
    return results.firstOrNull()
      ?: throw ConfigNotFoundException(configNotFoundType, cursorId.toString())
  }

  private fun getConnectionField(actorType: ActorType) = if (actorType == ActorType.source) "source_id" else "destination_id"
}
