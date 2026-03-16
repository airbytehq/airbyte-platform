/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("data_worker_usage_reservation")
data class DataWorkerUsageReservation(
  @field:Id
  var jobId: Long,
  var organizationId: UUID,
  var workspaceId: UUID,
  var dataplaneGroupId: UUID,
  var sourceCpuRequest: Double,
  var destinationCpuRequest: Double,
  var orchestratorCpuRequest: Double,
  var usedOnDemandCapacity: Boolean,
  var createdAt: OffsetDateTime,
)
