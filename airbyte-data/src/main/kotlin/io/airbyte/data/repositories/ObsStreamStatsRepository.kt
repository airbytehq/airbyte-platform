/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ObsStreamStats
import io.airbyte.data.repositories.entities.ObsStreamStatsId
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ObsStreamStatsRepository : PageableRepository<ObsStreamStats, ObsStreamStatsId> {
  @Query("SELECT * FROM observability_stream_stats WHERE job_id = :jobId")
  fun findByJobId(jobId: Long): List<ObsStreamStats>

  @Query(
    """
    WITH
      target_job AS MATERIALIZED (
          SELECT * FROM observability_jobs_stats WHERE job_id = :jobId LIMIT 1
      ),
      selected_jobs AS MATERIALIZED (
          SELECT ojs.job_id
          FROM observability_jobs_stats ojs
          JOIN target_job ON ojs.connection_id = target_job.connection_id
          WHERE
            ojs.created_at <= target_job.created_at
            AND ojs.created_at > (target_job.created_at - :intervalMinutes * (INTERVAL '1min'))
            AND ((:jobTypes) IS NULL OR ojs.job_type in (:jobTypes))
          ORDER BY ojs.created_at DESC
          LIMIT (:limit + 1)
        )
    SELECT oss.*
    FROM observability_stream_stats oss
    JOIN selected_jobs ON oss.job_id = selected_jobs.job_id
  """,
  )
  fun findJobWithPrevious(
    jobId: Long,
    intervalMinutes: Long,
    limit: Int,
    jobTypes: List<String>? = null,
  ): List<ObsStreamStats>

  @Query(
    """
    UPDATE observability_stream_stats
    SET
        bytes_loaded = :bytesLoaded,
        records_loaded = :recordsLoaded,
        records_rejected = :recordsRejected,
        was_backfilled = :wasBackfilled,
        was_resumed = :wasResumed
    WHERE
        job_id = :jobId AND
        stream_name = :streamName AND
        (stream_namespace = :streamNamespace OR (:streamNamespace IS NULL AND stream_namespace IS NULL))
  """,
  )
  fun updateAll(obsStreamStatsList: List<ObsStreamStats>): List<ObsStreamStats>
}
