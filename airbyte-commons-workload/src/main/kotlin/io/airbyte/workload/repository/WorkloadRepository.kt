/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

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

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
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
   * Heartbeat transitions a workload into a running state if the workload was claimed, launched or running and updates last heartbeat.
   * Succeed returns the workload if the status is running.
   */
  @Query(
    """
      UPDATE workload
      SET
       status = 'running',
       deadline = :deadline,
       last_heartbeat_at = now(),
       updated_at = now()
      WHERE id = :id AND status in ('claimed', 'launched', 'running')
      RETURNING *
    """,
  )
  fun heartbeat(
    @Id id: String,
    deadline: OffsetDateTime,
  ): Workload?

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
