/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
import java.time.OffsetDateTime
import java.util.UUID

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
    "SELECT DISTINCT ON (stream_name, stream_namespace, run_state, incomplete_run_cause, job_type) * " +
      "FROM stream_statuses WHERE connection_id = :connectionId " +
      "ORDER BY job_type, stream_name, stream_namespace, run_state, incomplete_run_cause, transitioned_at DESC",
  )
  abstract fun findAllPerRunStateByConnectionId(connectionId: UUID): List<StreamStatus>

  /**
   * Returns the latest terminal stream status per stream per day for a connection after a given
   * timestamp. Filters for complete and incomplete so we don't report an effectively unknown overview
   * status
   */
  @Query(
    (
      "SELECT DISTINCT ON (stream_name, stream_namespace, 1) DATE_TRUNC('day', transitioned_at, :timezone), * " +
        "FROM stream_statuses WHERE connection_id = :connectionId AND transitioned_at >= :timestamp " +
        "AND run_state IN ('complete', 'incomplete') " +
        "ORDER BY stream_name, stream_namespace, 1, transitioned_at DESC"
    ),
  )
  abstract fun findLatestTerminalStatusPerStreamByConnectionIdAndDayAfterTimestamp(
    connectionId: UUID,
    timestamp: OffsetDateTime,
    timezone: String,
  ): List<StreamStatus>

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
