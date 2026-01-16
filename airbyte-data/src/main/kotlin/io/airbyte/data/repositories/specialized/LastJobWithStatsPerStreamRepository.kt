/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.specialized

import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface LastJobWithStatsPerStreamRepository : GenericRepository<StreamWithLastJobId, Long> {
  /**
   * Given a connectionId and list of stream names/namespaces, fetch the latest job ID that has
   * stream_stats for each stream in that connection.
   */
  @Query(
    """
      WITH statuses_with_fallbacks AS (
        SELECT
          job_id,
          stream_namespace,
          stream_name,
          -- when the associated job is no longer running, compute a terminal run_state from the job's status
          CASE
            WHEN j.status in ('pending', 'running', 'incomplete') THEN ss.run_state
            WHEN ss.run_state not in ('complete', 'incomplete') and j.status = 'succeeded' THEN 'complete'
            WHEN ss.run_state not in ('complete', 'incomplete') THEN 'incomplete'
            ELSE ss.run_state
          END as run_state
        FROM
          stream_statuses ss
        JOIN jobs j ON j.id = ss.job_id
        WHERE
          connection_id = :connectionId
      )
      SELECT
        MAX(job_id) as job_id, stream_namespace, stream_name
      FROM statuses_with_fallbacks
      WHERE run_state IN ('complete', 'incomplete')
      GROUP BY
        stream_namespace,
        stream_name
    """,
    readOnly = true,
  )
  fun findLastJobIdWithStatsPerStream(connectionId: UUID): List<StreamWithLastJobId>
}

@Introspected
data class StreamWithLastJobId(
  val jobId: Long,
  val streamName: String,
  val streamNamespace: String?,
)
