/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataWorkerUsageRepository : PageableRepository<DataWorkerUsage, UUID> {
  @Query(
    """
    SELECT * from data_worker_usage
    WHERE organization_id = :organizationId
    AND bucket_start >= :startDate AND bucket_start <= :endDate
    """,
  )
  fun findByOrganizationIdAndJobStartBetween(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage>

  @Query(
    """
    INSERT INTO data_worker_usage (
      organization_id,
      workspace_id,
      dataplane_group_id,
      bucket_start,
      source_cpu_request,
      destination_cpu_request,
      orchestrator_cpu_request
    ) VALUES (
      :organizationId,
      :workspaceId,
      :dataplaneGroupId,
      DATE_TRUNC('hour', :jobStart::TIMESTAMP),
      :sourceCpuRequest,
      :destinationCpuRequest,
      :orchestratorCpuRequest
    ) ON CONFLICT (organization_id, bucket_start, workspace_id, dataplane_group_id) DO UPDATE
    SET 
    source_cpu_request = data_worker_usage.source_cpu_request + EXCLUDED.source_cpu_request,
    destination_cpu_request = data_worker_usage.destination_cpu_request + EXCLUDED.destination_cpu_request,
    orchestrator_cpu_request = data_worker_usage.orchestrator_cpu_request + EXCLUDED.orchestrator_cpu_request
    """,
  )
  fun insert(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    jobStart: OffsetDateTime,
    sourceCpuRequest: Double,
    destinationCpuRequest: Double,
    orchestratorCpuRequest: Double,
  )
}
