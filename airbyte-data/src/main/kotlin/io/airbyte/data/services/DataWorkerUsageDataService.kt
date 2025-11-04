/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.entities.DataWorkerUsage
import java.time.OffsetDateTime
import java.util.UUID

interface DataWorkerUsageDataService {
  fun findMostRecentUsageBucket(
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupID: UUID,
    bucketStart: OffsetDateTime,
  ): DataWorkerUsage?

  fun insertNewDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage)

  fun incrementExistingDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage)

  fun decrementExistingDataWorkerUsageBucket(dataWorkerUsage: DataWorkerUsage)

  fun getDataWorkerUsageByOrganizationAndTimeRange(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage>
}
