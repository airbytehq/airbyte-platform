/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ObsJobsStats
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ObsJobsStatsRepository : PageableRepository<ObsJobsStats, Long> {
  @Query(
    """
    WITH target_job AS MATERIALIZED (
	      SELECT * FROM observability_jobs_stats WHERE job_id = :jobId LIMIT 1
    )
    SELECT *
    FROM observability_jobs_stats ojs
    JOIN target_job ON ojs.connection_id = target_job.connection_id
    WHERE
      ojs.created_at <= target_job.created_at
      AND ojs.created_at > (target_job.created_at - :intervalMinutes * (INTERVAL '1min'))
      AND ((:jobTypes) IS NULL OR ojs.job_type in (:jobTypes))
    ORDER BY ojs.created_at DESC
    LIMIT (:limit + 1)
  """,
  )
  fun findJobWithPrevious(
    jobId: Long,
    intervalMinutes: Long,
    limit: Int,
    jobTypes: List<String>? = null,
  ): List<ObsJobsStats>
}
