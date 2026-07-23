/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.CrudRepository
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
}
