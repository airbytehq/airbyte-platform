package io.airbyte.workload.repository

import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.micronaut.data.annotation.Expandable
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.Join
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.Optional

@JdbcRepository(dialect = Dialect.POSTGRES)
interface WorkloadRepository : PageableRepository<Workload, String> {
  @Join(value = "workloadLabels", type = Join.Type.LEFT_FETCH)
  override fun findById(
    @Id id: String,
  ): Optional<Workload>

  @Query(
    """
      SELECT * FROM workload
      WHERE ((:dataplaneIds) IS NULL OR dataplane_id IN (:dataplaneIds))
      AND ((:statuses) IS NULL OR status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND (CAST(:updatedBefore AS timestamptz) IS NULL OR updated_at < CAST(:updatedBefore AS timestamptz))
      """,
  )
  fun search(
    @Expandable dataplaneIds: List<String>?,
    @Expandable statuses: List<WorkloadStatus>?,
    updatedBefore: OffsetDateTime?,
  ): List<Workload>

  @Query(
    """
      SELECT * FROM workload
      WHERE ((:dataplaneIds) IS NULL OR dataplane_id IN (:dataplaneIds))
      AND ((:statuses) IS NULL OR status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND ((:types) IS NULL OR type = ANY(CAST(ARRAY[:types] AS workload_type[])))
      AND (CAST(:createdBefore AS timestamptz) IS NULL OR created_at < CAST(:createdBefore AS timestamptz))
      """,
  )
  fun searchByTypeStatusAndCreationDate(
    @Expandable dataplaneIds: List<String>?,
    @Expandable statuses: List<WorkloadStatus>?,
    @Expandable types: List<WorkloadType>?,
    createdBefore: OffsetDateTime?,
  ): List<Workload>

  fun update(
    @Id id: String,
    status: WorkloadStatus,
  )

  fun update(
    @Id id: String,
    status: WorkloadStatus,
    terminationSource: String?,
    terminationReason: String?,
  )

  fun update(
    @Id id: String,
    status: WorkloadStatus,
    lastHeartbeatAt: OffsetDateTime,
  )

  fun update(
    @Id id: String,
    dataplaneId: String,
    status: WorkloadStatus,
  )
}
