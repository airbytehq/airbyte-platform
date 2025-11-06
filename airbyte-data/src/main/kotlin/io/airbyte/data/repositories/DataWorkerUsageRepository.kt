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
      AND bucket_start >= :startDate::TIMESTAMPTZ AT TIME ZONE 'UTC'
      AND bucket_start <= :endDate::TIMESTAMPTZ AT TIME ZONE 'UTC'
    """,
  )
  fun findByOrganizationIdAndJobStartBetween(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage>

  @Query(
    """
      SELECT * FROM data_worker_usage
      WHERE organization_id = :organizationId
        AND workspace_id = :workspaceId
        AND dataplane_group_id = :dataplaneGroupId
        AND bucket_start <= :bucketStart::TIMESTAMPTZ AT TIME ZONE 'UTC'
      ORDER BY bucket_start DESC
      LIMIT 1
      FOR UPDATE
    """,
  )
  fun findMostRecentUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    bucketStart: OffsetDateTime,
  ): DataWorkerUsage?

  @Query(
    """
    UPDATE data_worker_usage
    SET
      source_cpu_request = source_cpu_request + :sourceCpuRequest,
      destination_cpu_request = destination_cpu_request + :destinationCpuRequest,
      orchestrator_cpu_request = orchestrator_cpu_request + :orchestratorCpuRequest,
      max_source_cpu_request = GREATEST(max_source_cpu_request, source_cpu_request + :sourceCpuRequest),
      max_destination_cpu_request = GREATEST(max_destination_cpu_request, destination_cpu_request + :destinationCpuRequest),
      max_orchestrator_cpu_request = GREATEST(max_orchestrator_cpu_request, orchestrator_cpu_request + :orchestratorCpuRequest)
    WHERE organization_id = :organizationId
      AND workspace_id = :workspaceId
      AND dataplane_group_id = :dataplaneGroupId
      AND bucket_start = DATE_TRUNC('hour', :bucketStart::TIMESTAMPTZ AT TIME ZONE 'UTC')
    """,
  )
  fun incrementExistingDataWorkerUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    bucketStart: OffsetDateTime,
    sourceCpuRequest: Double,
    destinationCpuRequest: Double,
    orchestratorCpuRequest: Double,
  )

  @Query(
    """
      INSERT INTO data_worker_usage (
      organization_id,
      workspace_id,
      dataplane_group_id,
      bucket_start,
      source_cpu_request,
      destination_cpu_request,
      orchestrator_cpu_request,
      max_source_cpu_request,
      max_destination_cpu_request,
      max_orchestrator_cpu_request
    ) VALUES (
    :organizationId,
    :workspaceId,
    :dataplaneGroupId,
    DATE_TRUNC('hour', :bucketStart::TIMESTAMPTZ AT TIME ZONE 'UTC'),
    :sourceCpuRequest,
    :destinationCpuRequest,
    :orchestratorCpuRequest,
    :maxSourceCpuRequest,
    :maxDestinationCpuRequest,
    :maxOrchestratorCpuRequest
    )
    """,
  )
  fun insertNewDataWorkerUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    bucketStart: OffsetDateTime,
    sourceCpuRequest: Double,
    destinationCpuRequest: Double,
    orchestratorCpuRequest: Double,
    maxSourceCpuRequest: Double,
    maxDestinationCpuRequest: Double,
    maxOrchestratorCpuRequest: Double,
  )

  @Query(
    """
    UPDATE data_worker_usage
    SET
      source_cpu_request = GREATEST(0.0, data_worker_usage.source_cpu_request - :sourceCpuRequest),
      destination_cpu_request = GREATEST(0.0, data_worker_usage.destination_cpu_request - :destinationCpuRequest),
      orchestrator_cpu_request = GREATEST(0.0, data_worker_usage.orchestrator_cpu_request - :orchestratorCpuRequest)
    WHERE organization_id = :organizationId
      AND workspace_id = :workspaceId
      AND dataplane_group_id = :dataplaneGroupId
      AND bucket_start = DATE_TRUNC('hour', :bucketStart::TIMESTAMPTZ AT TIME ZONE 'UTC')
    """,
  )
  fun decrementExistingDataWorkerUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    bucketStart: OffsetDateTime,
    sourceCpuRequest: Double,
    destinationCpuRequest: Double,
    orchestratorCpuRequest: Double,
  )
}
