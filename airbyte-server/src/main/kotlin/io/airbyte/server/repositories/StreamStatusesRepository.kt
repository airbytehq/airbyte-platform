/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import com.google.common.base.CaseFormat
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.server.repositories.domain.StreamStatus
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.Page
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import io.micronaut.data.repository.jpa.JpaSpecificationExecutor
import io.micronaut.data.repository.jpa.criteria.PredicateSpecification
import jakarta.persistence.criteria.CriteriaBuilder
import jakarta.persistence.criteria.Root
import org.jooq.TableField
import java.util.UUID

const val STREAM_STATUS_WITH_FALLBACKS = """
-- when the associated job is no longer running, compute a terminal run_state from the job's status
CASE
  WHEN j.status in ('pending', 'running', 'incomplete') THEN ss.run_state
  WHEN ss.run_state not in ('complete', 'incomplete') and j.status = 'succeeded' THEN 'complete'
  WHEN ss.run_state not in ('complete', 'incomplete') THEN 'incomplete'
  ELSE ss.run_state
END as run_state,
CASE
  WHEN j.status in ('pending', 'running', 'incomplete') THEN ss.incomplete_run_cause
  WHEN ss.run_state not in ('complete', 'incomplete') and j.status = 'succeeded' THEN null
  WHEN ss.run_state not in ('complete', 'incomplete') THEN 'failed'
  ELSE ss.incomplete_run_cause
END as incomplete_run_cause,
-- the rest of fields for StreamStatus
ss.id,
ss.workspace_id,
ss.connection_id,
ss.job_id,
ss.stream_namespace,
ss.stream_name,
coalesce(ss.created_at, j.created_at) as created_at, -- coalesce in case this is a NULL row
coalesce(ss.updated_at, j.updated_at) as updated_at, -- coalesce in case this is a NULL row
ss.attempt_number,
ss.job_type,
ss.run_state,
coalesce(ss.transitioned_at, j.created_at) as transitioned_at, -- coalesce in case this is a NULL row
ss.metadata
"""

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
abstract class StreamStatusesRepository : PageableRepository<StreamStatus, UUID>, JpaSpecificationExecutor<StreamStatus> {
  /**
   * Returns stream statuses filtered by the provided params.
   */
  open fun findAllFiltered(params: FilterParams): Page<StreamStatus> {
    var spec: PredicateSpecification<StreamStatus> =
      Predicates.columnEquals(Columns.WORKSPACE_ID, params.workspaceId)
    var pageable = Pageable.unpaged()

    if (null != params.connectionId) {
      spec = spec.and(Predicates.columnEquals(Columns.CONNECTION_ID, params.connectionId))
    }

    if (null != params.jobId) {
      spec = spec.and(Predicates.columnEquals(Columns.JOB_ID, params.jobId))
    }

    if (null != params.streamNamespace) {
      spec =
        spec.and(Predicates.columnEquals(Columns.STREAM_NAMESPACE, params.streamNamespace))
    }

    if (null != params.streamName) {
      spec = spec.and(Predicates.columnEquals(Columns.STREAM_NAME, params.streamName))
    }

    if (null != params.attemptNumber) {
      spec = spec.and(Predicates.columnEquals(Columns.ATTEMPT_NUMBER, params.attemptNumber))
    }

    if (null != params.jobType) {
      spec =
        spec.and(Predicates.columnEquals(Columns.JOB_TYPE, params.jobType))
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

  /**
   * Pagination params.
   */
  data class Pagination(
    val offset: Int,
    val size: Int,
  )

  /**
   * Params for filtering our list functionality.
   */
  data class FilterParams(
    val workspaceId: UUID?,
    val connectionId: UUID?,
    val jobId: Long?,
    val streamNamespace: String?,
    val streamName: String?,
    val attemptNumber: Int?,
    val jobType: JobStreamStatusJobType?,
    val pagination: Pagination?,
  )

  /**
   * Predicates for dynamic query building. Portable.
   */
  object Predicates {
    /*
     * Jooq holds onto the names of the columns in snake_case, so we have to convert to lower camelCase
     * for the JpaSpecificationExecutor to do predicate filtering.
     */
    fun formatJooqColumnName(jooqColumn: TableField<*, *>): String {
      return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, jooqColumn.name)
    }

    fun <U> columnEquals(
      columnName: String,
      value: U,
    ): PredicateSpecification<StreamStatus> {
      return PredicateSpecification { root: Root<StreamStatus>, criteriaBuilder: CriteriaBuilder ->
        criteriaBuilder.equal(
          root.get<StreamStatus>(columnName),
          value,
        )
      }
    }
  }

  /**
   * Column names for StreamStatus in camel case for the JpaSpecificationExecutor. In lieu of a
   * metamodel, we pre-create JPA-friendly column names from the already generated Jooq model.
   */
  object Columns {
    val WORKSPACE_ID: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.WORKSPACE_ID)
    val CONNECTION_ID: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.CONNECTION_ID)
    val JOB_ID: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.JOB_ID)
    val STREAM_NAMESPACE: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.STREAM_NAMESPACE)
    val STREAM_NAME: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.STREAM_NAME)
    val ATTEMPT_NUMBER: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.ATTEMPT_NUMBER)
    val JOB_TYPE: String = Predicates.formatJooqColumnName(Tables.STREAM_STATUSES.JOB_TYPE)
  }
}
