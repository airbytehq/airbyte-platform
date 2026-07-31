/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.CrudRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataWorkerUsageReservationRepository : CrudRepository<DataWorkerUsageReservation, Long> {
  @Query(
    """
      SELECT COALESCE(SUM(
        r.source_cpu_request + r.destination_cpu_request + r.orchestrator_cpu_request
      ), 0.0)
      FROM data_worker_usage_reservation r
      JOIN jobs j ON j.id = r.job_id
      WHERE r.organization_id = :organizationId
        AND j.status IN ('pending', 'queued', 'running', 'incomplete')
    """,
  )
  fun sumReservedCpuForActiveJobsByOrganizationId(organizationId: UUID): Double

  /**
   * Atomically reserves capacity for a job only if that job is still in a non-terminal state.
   *
   * The reservation row is inserted in the same statement that checks the job status, closing the
   * release-before-reserve race: if the job already reached a terminal state (and its release path
   * therefore already ran and found no reservation to free), the WHERE EXISTS yields no row and
   * nothing is inserted. `ON CONFLICT DO NOTHING` makes the insert idempotent for retried reserves.
   *
   * The active-status list MUST stay in lockstep with [sumReservedCpuForActiveJobsByOrganizationId]
   * above — both define what "active" means for Data Worker accounting.
   *
   * @return the number of rows inserted: 1 when a reservation was newly created for an active job,
   *         0 when the job is already terminal or a reservation already existed.
   */
  @Query(
    """
      INSERT INTO data_worker_usage_reservation (
        job_id, organization_id, workspace_id, dataplane_group_id,
        source_cpu_request, destination_cpu_request, orchestrator_cpu_request,
        used_on_demand_capacity, created_at
      )
      SELECT :jobId, :organizationId, :workspaceId, :dataplaneGroupId,
             :sourceCpuRequest, :destinationCpuRequest, :orchestratorCpuRequest,
             :usedOnDemandCapacity, :createdAt
      WHERE EXISTS (
        SELECT 1 FROM jobs j
        WHERE j.id = :jobId
          AND j.status IN ('pending', 'queued', 'running', 'incomplete')
      )
      ON CONFLICT (job_id) DO NOTHING
    """,
  )
  fun insertReservationIfJobActive(
    jobId: Long,
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    sourceCpuRequest: Double,
    destinationCpuRequest: Double,
    orchestratorCpuRequest: Double,
    usedOnDemandCapacity: Boolean,
    createdAt: OffsetDateTime,
  ): Int
}
