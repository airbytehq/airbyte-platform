/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.DataWorkerUsageRepository
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.services.DataWorkerUsageDataService
import io.micronaut.data.exceptions.DataAccessException
import jakarta.inject.Singleton
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID

@Singleton
class DataWorkerUsageServiceDataImpl(
  private val repository: DataWorkerUsageRepository,
) : DataWorkerUsageDataService {
  override fun insertDataWorkerUsage(dataWorkerUsage: DataWorkerUsage) {
    repository.insert(
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
