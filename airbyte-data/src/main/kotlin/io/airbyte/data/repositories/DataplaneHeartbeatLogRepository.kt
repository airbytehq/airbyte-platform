/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataplaneHeartbeatLogRepository : PageableRepository<DataplaneHeartbeatLog, UUID> {
  fun save(log: DataplaneHeartbeatLog): DataplaneHeartbeatLog

  @Query(
    """
    SELECT DISTINCT ON (dataplane_id) *
    FROM dataplane_heartbeat_log
    WHERE dataplane_id IN (:dataplaneIds)
    ORDER BY dataplane_id, created_at DESC
    """,
  )
  fun findLatestHeartbeatsByDataplaneIds(dataplaneIds: List<UUID>): List<DataplaneHeartbeatLog>

  @Query(
    """
    SELECT * FROM dataplane_heartbeat_log
    WHERE dataplane_id = :dataplaneId
    AND created_at >= :startTime
    AND created_at <= :endTime
    ORDER BY created_at DESC
    """,
  )
  fun findHeartbeatHistory(
    dataplaneId: UUID,
    startTime: OffsetDateTime,
    endTime: OffsetDateTime,
  ): List<DataplaneHeartbeatLog>

  @Query(
    """
    SELECT * FROM dataplane_heartbeat_log
    WHERE dataplane_id IN (:dataplaneIds)
    AND created_at >= :startTime
    AND created_at <= :endTime
    ORDER BY dataplane_id, created_at DESC
    """,
  )
  fun findHeartbeatHistoryForDataplanes(
    dataplaneIds: List<UUID>,
    startTime: OffsetDateTime,
    endTime: OffsetDateTime,
  ): List<DataplaneHeartbeatLog>

  @Query(
    """
    WITH latest_heartbeats AS (
      SELECT DISTINCT ON (dataplane_id) id
      FROM dataplane_heartbeat_log
      ORDER BY dataplane_id, created_at DESC
    )
    DELETE FROM dataplane_heartbeat_log dhl
    WHERE dhl.created_at < :cutoffTime
      AND NOT EXISTS (
        SELECT 1 FROM latest_heartbeats lh WHERE lh.id = dhl.id
      )
    """,
  )
  fun deleteOldHeartbeatsExceptLatest(cutoffTime: OffsetDateTime): Int
}
