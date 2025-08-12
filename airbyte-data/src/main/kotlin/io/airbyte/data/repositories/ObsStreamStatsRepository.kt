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
