/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.entities.DataWorkerUsage
import java.time.OffsetDateTime
import java.util.UUID

interface DataWorkerUsageDataService {
  fun insertDataWorkerUsage(dataWorkerUsage: DataWorkerUsage)

  fun getDataWorkerUsageByOrganizationAndTimeRange(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage>
}
