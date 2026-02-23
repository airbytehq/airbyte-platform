/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import io.airbyte.server.repositories.domain.StreamStatus
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.Page
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import io.micronaut.data.repository.jpa.JpaSpecificationExecutor
import java.util.UUID

/**
 * Read-only repository for stream statuses that uses the replica datasource.
 * This repository should be used for read operations when the read replica feature flag is enabled.
 *
 * When the feature flag is disabled, use [StreamStatusesRepository] for both read and write operations.
 * For write operations, always use [StreamStatusesRepository].
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config-replica")
abstract class StreamStatusesReadRepository :
  PageableRepository<StreamStatus, UUID>,
  JpaSpecificationExecutor<StreamStatus> {
  /**
   * Returns stream statuses filtered by the provided params.
   */
  open fun findAllFiltered(params: StreamStatusesRepository.FilterParams): Page<StreamStatus> {
    var spec =
      StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.WORKSPACE_ID, params.workspaceId)
    var pageable = Pageable.unpaged()

    if (null != params.connectionId) {
      spec = spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.CONNECTION_ID, params.connectionId))
    }

    if (null != params.jobId) {
      spec = spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.JOB_ID, params.jobId))
    }

    if (null != params.streamNamespace) {
      spec =
        spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.STREAM_NAMESPACE, params.streamNamespace))
    }

    if (null != params.streamName) {
      spec = spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.STREAM_NAME, params.streamName))
    }

    if (null != params.attemptNumber) {
      spec = spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.ATTEMPT_NUMBER, params.attemptNumber))
    }

    if (null != params.jobType) {
      spec =
        spec.and(StreamStatusesRepository.Predicates.columnEquals(StreamStatusesRepository.Columns.JOB_TYPE, params.jobType))
    }

    if (null != params.pagination) {
      val offset: Int = params.pagination.offset
      val size: Int = params.pagination.size
      pageable = Pageable.from(offset, size)
    }

    return findAll(spec, pageable)
  }

  /**
   * Returns the latest stream status per run state (and job type) for a connection.
   */
  @Query(
    """
    SELECT DISTINCT ON (ss.stream_name, ss.stream_namespace, 1 /* computed run_state */, 2 /* computed incomplete_run_cause */, ss.job_type)
      $STREAM_STATUS_WITH_FALLBACKS
      FROM stream_statuses ss
      JOIN jobs j on j.id = ss.job_id
      WHERE ss.connection_id = :connectionId
      ORDER BY
        ss.job_type, ss.stream_name, ss.stream_namespace,
        1 /* computed run_state */,2 /* computed incomplete_run_cause */, ss.transitioned_at desc
      """,
  )
  abstract fun findAllPerRunStateByConnectionId(connectionId: UUID): List<StreamStatus>

  /**
   * Returns the latest stream status per run state (and job type) for a connection,
   * limited to recent jobs for better performance on high-volume connections.
   *
   * This is the same logic as findAllPerRunStateByConnectionId but with a recency filter
   * that limits processing to the last N jobs, reducing query time from 2+ minutes to ~18 seconds
   * for connections with millions of stream_statuses.
   */
  @Query(
    """
    SELECT DISTINCT ON (ss.stream_name, ss.stream_namespace, 1 /* computed run_state */, 2 /* computed incomplete_run_cause */, ss.job_type)
      $STREAM_STATUS_WITH_FALLBACKS
      FROM stream_statuses ss
      JOIN jobs j on j.id = ss.job_id
      WHERE ss.connection_id = :connectionId
        AND ss.job_id >= (
          SELECT COALESCE(MIN(id), 0)
          FROM (
            SELECT id FROM jobs
            WHERE scope = CAST(:connectionId AS VARCHAR)
              AND config_type IN ('sync', 'refresh')
            ORDER BY id DESC
            LIMIT :recentJobsLimit
          ) recent_jobs
        )
      ORDER BY
        ss.job_type, ss.stream_name, ss.stream_namespace,
        1 /* computed run_state */,2 /* computed incomplete_run_cause */, ss.transitioned_at desc
      """,
  )
  abstract fun findAllPerRunStateByConnectionIdWithRecentJobsFilter(
    connectionId: UUID,
    recentJobsLimit: Int,
  ): List<StreamStatus>

  /**
   * Returns stream statuses for the last X jobs for a connection.
   */
  @Query(
    """
           SELECT
              j.id as job_id,
              $STREAM_STATUS_WITH_FALLBACKS,
              last_x_jobs.created_at AS job_created_at, last_x_jobs.updated_at AS job_updated_at
           FROM jobs j
           -- left join to stream status, as there may not be a stream status for this job but we still need the job_id
           LEFT JOIN (
              SELECT ss.*
              FROM stream_statuses ss
              -- limit to the last attempt in the job for this stream
              JOIN (
                 SELECT job_id, MAX(attempt_number) AS max_attempt_number
                 FROM stream_statuses
                 WHERE connection_id = :connectionId
                 GROUP BY job_id
              ) AS max_attempts ON ss.job_id = max_attempts.job_id AND ss.attempt_number = max_attempts.max_attempt_number
           ) as ss on ss.job_id = j.id
           INNER JOIN (
               SELECT id, created_at, updated_at
               FROM jobs
               WHERE config_type in ('sync', 'refresh') AND status in ('succeeded', 'failed') AND scope = CAST(:connectionId AS VARCHAR)
               ORDER BY created_at DESC
               LIMIT :lastXJobs
             ) AS last_x_jobs ON j.id = last_x_jobs.id
           WHERE j.scope = (:connectionId :: varchar);
         """,
  )
  abstract fun findLastAttemptsOfLastXJobsForConnection(
    connectionId: UUID?,
    lastXJobs: Int,
  ): List<StreamStatus?>?
}
