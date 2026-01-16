/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.StreamStats
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface StreamStatsRepository : PageableRepository<StreamStats, UUID> {
  @Query(
    """
    SELECT ss.id, ss.attempt_id, ss.stream_name, ss.stream_namespace, ss.records_emitted,
           ss.bytes_emitted, ss.estimated_records, ss.estimated_bytes, ss.created_at,
           ss.updated_at, ss.bytes_committed, ss.records_committed, ss.connection_id,
           ss.records_rejected, ss.additional_stats, sam.was_backfilled, sam.was_resumed
    FROM stream_stats ss
    LEFT JOIN stream_attempt_metadata sam
      ON ss.attempt_id = sam.attempt_id
      AND ss.stream_name = sam.stream_name
      AND (ss.stream_namespace = sam.stream_namespace
           OR (ss.stream_namespace IS NULL AND sam.stream_namespace IS NULL))
    WHERE ss.attempt_id = :attemptId
    """,
  )
  fun findByAttemptId(attemptId: Long): List<StreamStats>

  @Query(
    """
    SELECT ss.id, ss.attempt_id, ss.stream_name, ss.stream_namespace, ss.records_emitted,
           ss.bytes_emitted, ss.estimated_records, ss.estimated_bytes, ss.created_at,
           ss.updated_at, ss.bytes_committed, ss.records_committed, ss.connection_id,
           ss.records_rejected, ss.additional_stats, sam.was_backfilled, sam.was_resumed
    FROM stream_stats ss
    INNER JOIN attempts a ON ss.attempt_id = a.id
    LEFT JOIN stream_attempt_metadata sam
      ON ss.attempt_id = sam.attempt_id
      AND ss.stream_name = sam.stream_name
      AND (ss.stream_namespace = sam.stream_namespace
           OR (ss.stream_namespace IS NULL AND sam.stream_namespace IS NULL))
    WHERE a.job_id = :jobId
    """,
  )
  fun findByJobId(jobId: Long): List<StreamStats>
}
