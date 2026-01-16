/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.DataWorkerUsageRepository
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.services.DataWorkerUsageDataService
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

@Singleton
class DataWorkerUsageServiceDataImpl(
  private val repository: DataWorkerUsageRepository,
) : DataWorkerUsageDataService {
  override fun findMostRecentUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupID: UUID,
    bucketStart: OffsetDateTime,
  ): DataWorkerUsage? = repository.findMostRecentUsageBucket(organizationId, workspaceId, dataplaneGroupID, bucketStart)

  override fun insertNewDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage) {
    repository.insertNewDataWorkerUsageBucket(
      dataWorkerUsage.organizationId,
      dataWorkerUsage.workspaceId,
      dataWorkerUsage.dataplaneGroupId,
      dataWorkerUsage.bucketStart,
      dataWorkerUsage.sourceCpuRequest,
      dataWorkerUsage.destinationCpuRequest,
      dataWorkerUsage.orchestratorCpuRequest,
      dataWorkerUsage.maxSourceCpuRequest,
      dataWorkerUsage.maxDestinationCpuRequest,
      dataWorkerUsage.maxOrchestratorCpuRequest,
    )
  }

  override fun incrementExistingDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage) {
    repository.incrementExistingDataWorkerUsageBucket(
      dataWorkerUsage.organizationId,
      dataWorkerUsage.workspaceId,
      dataWorkerUsage.dataplaneGroupId,
      dataWorkerUsage.bucketStart,
      dataWorkerUsage.sourceCpuRequest,
      dataWorkerUsage.destinationCpuRequest,
      dataWorkerUsage.orchestratorCpuRequest,
    )
  }

  override fun decrementExistingDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage) {
    repository.decrementExistingDataWorkerUsageBucket(
      dataWorkerUsage.organizationId,
      dataWorkerUsage.workspaceId,
      dataWorkerUsage.dataplaneGroupId,
      dataWorkerUsage.bucketStart,
      dataWorkerUsage.sourceCpuRequest,
      dataWorkerUsage.destinationCpuRequest,
      dataWorkerUsage.orchestratorCpuRequest,
    )
  }

  /**
   * Returns data worker usage by inclusive range (ie. both the start date and end date
   * are included in the results).
   */
  override fun getDataWorkerUsageByOrganizationAndTimeRange(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage> = repository.findByOrganizationIdAndJobStartBetween(organizationId, startDate, endDate)
}
