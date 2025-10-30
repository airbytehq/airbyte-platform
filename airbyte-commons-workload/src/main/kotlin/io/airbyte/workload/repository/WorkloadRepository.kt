/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository

import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadSummaryDTO
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

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface WorkloadRepository : PageableRepository<Workload, String> {
  @Join(value = "workloadLabels", type = Join.Type.LEFT_FETCH)
  override fun findById(
    @Id id: String,
  ): Optional<Workload>

  /**
   * Find workload by ID without joining on workload_label table.
   * Uses only the labels JSONB column from the workload table for better performance.
   */
  @Query(
    """
      SELECT * FROM workload WHERE id = :id
    """,
  )
  fun findByIdWithoutLegacyLabels(
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

  /**
   * Search for active workloads.
   *
   * Active workloads in this context have the expectation that the deadline is not null. This is to leverage the deadline index to avoid a
   * full table scan to find active workloads.
   */
  @Query(
    """
      SELECT id, status, deadline, auto_id FROM workload
      WHERE ((:dataplaneIds) IS NULL OR dataplane_id IN (:dataplaneIds))
      AND ((:statuses) IS NULL OR status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND (deadline IS NOT NULL)
    """,
  )
  fun searchActive(
    @Expandable dataplaneIds: List<String>?,
    @Expandable statuses: List<WorkloadStatus>? = null,
  ): List<WorkloadSummaryDTO>

  @Query(
    """
      SELECT * FROM workload
      WHERE ((:dataplaneIds) IS NULL OR dataplane_id IN (:dataplaneIds))
      AND ((:statuses) IS NULL OR status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND (deadline < CAST(:deadline AS timestamptz))
      """,
  )
  fun searchForExpiredWorkloads(
    @Expandable dataplaneIds: List<String>?,
    @Expandable statuses: List<WorkloadStatus>?,
    deadline: OffsetDateTime,
  ): List<Workload>

  fun searchByMutexKeyAndStatusInList(
    mutexKey: String,
    statuses: List<WorkloadStatus>,
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

  @Query(
    """
      SELECT * FROM workload
      WHERE deadline IS NOT NULL
      AND ((:dataplaneIds) IS NULL OR dataplane_id IN (:dataplaneIds))
      AND ((:statuses) IS NULL OR status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND ((:types) IS NULL OR type = ANY(CAST(ARRAY[:types] AS workload_type[])))
      AND (CAST(:createdBefore AS timestamptz) IS NULL OR created_at < CAST(:createdBefore AS timestamptz))
      """,
  )
  fun searchByTypeStatusAndCreationDateWithDeadline(
    @Expandable dataplaneIds: List<String>?,
    @Expandable statuses: List<WorkloadStatus>?,
    @Expandable types: List<WorkloadType>?,
    createdBefore: OffsetDateTime?,
  ): List<Workload>

  /**
   * Find workloads by connection ID and statuses.
   * Filters using the labels JSONB column or legacy workload_label table.
   * Used for administrative operations like force cleanup.
   *
   * IMPORTANT: This method is NOT optimized for high-frequency production use.
   * The connection_id filtering is not indexed (no GIN index on labels JSONB column),
   * which means queries will perform JSONB extraction on all rows matching the status filter.
   * This is acceptable for infrequent administrative/break-glass operations but would
   * degrade performance if used in high-volume production code paths.
   */
  @Query(
    """
      SELECT DISTINCT w.* FROM workload w
      LEFT JOIN workload_label wl ON w.id = wl.workload_id
      WHERE ((:statuses) IS NULL OR w.status = ANY(CAST(ARRAY[:statuses] AS workload_status[])))
      AND (w.labels->>'connection_id' = :connectionId
           OR (wl.key = 'connection_id' AND wl.value = :connectionId))
    """,
  )
  fun findByConnectionIdAndStatuses(
    connectionId: String,
    @Expandable statuses: List<WorkloadStatus>?,
  ): List<Workload>

  /**
   * Cancel transitions a workload into a cancelled state if the workload was non-terminal.
   * Cancel returns the workload if the status was just updated to cancelled.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'cancelled',
       termination_reason = :reason,
       termination_source = :source,
       deadline = null,
       updated_at = now()
      WHERE id = :id AND status in ('pending', 'claimed', 'launched', 'running')
      RETURNING *
    """,
  )
  fun cancel(
    @Id id: String,
    reason: String?,
    source: String?,
  ): Workload?

  /**
   * Claim transitions a workload into a claimed state and updates the deadline if the workload was pending.
   * Claim returns the workload if it is in a valid claimed status by the dataplane (either from this call or if it was already claimed).
   */
  @Query(
    """
      UPDATE workload
      SET
       dataplane_id = :dataplaneId,
       status = 'claimed',
       deadline = case
                    when status = 'pending' then :deadline
                    else deadline
                  end,
       updated_at = now()
      WHERE id = :id AND (status = 'pending' OR (status = 'claimed' AND dataplane_id = :dataplaneId))
      RETURNING *
    """,
  )
  fun claim(
    @Id id: String,
    dataplaneId: String,
    deadline: OffsetDateTime,
  ): Workload?

  /**
   * Fail transitions a workload into a cancelled state if the workload was non-terminal.
   * Fail returns the workload if the status was just updated to failure.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'failure',
       termination_reason = :reason,
       termination_source = :source,
       deadline = null,
       updated_at = now()
      WHERE id = :id AND status in ('pending', 'claimed', 'launched', 'running')
      RETURNING *
    """,
  )
  fun fail(
    @Id id: String,
    reason: String?,
    source: String?,
  ): Workload?

  /**
   * Heartbeat updates last heartbeat for a workload already in running state.
   * Does NOT change status - callers must call running() first to transition to running state.
   * Returns the number of rows updated (1 if successful, 0 if workload not found or not in running state).
   */
  @Query(
    """
      UPDATE workload
      SET
       deadline = :deadline,
       last_heartbeat_at = now(),
       updated_at = now()
      WHERE id = :id AND status = 'running'
    """,
  )
  fun heartbeat(
    @Id id: String,
    deadline: OffsetDateTime,
  ): Int

  /**
   * Launch transitions a workload into a launched state if the workload was claimed or launched.
   * Succeed returns the workload if the status is launched.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'launched',
       deadline = :deadline,
       updated_at = now()
      WHERE id = :id AND status in ('claimed', 'launched')
      RETURNING *
    """,
  )
  fun launch(
    @Id id: String,
    deadline: OffsetDateTime,
  ): Workload?

  /**
   * Running transitions a workload into a running state if the workload was claimed, launched or running.
   * Succeed returns the workload if the status is launched.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'running',
       deadline = :deadline,
       updated_at = now()
      WHERE id = :id AND status in ('claimed', 'launched', 'running')
      RETURNING *
    """,
  )
  fun running(
    @Id id: String,
    deadline: OffsetDateTime,
  ): Workload?

  /**
   * Succeed transitions a workload into a cancelled state if the workload was non-terminal.
   * Succeed returns the workload if the status was just updated to success.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'success',
       deadline = null,
       updated_at = now()
      WHERE id = :id AND status in ('pending', 'claimed', 'launched', 'running')
      RETURNING *
    """,
  )
  fun succeed(
    @Id id: String,
  ): Workload?
}
