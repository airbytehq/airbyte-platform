/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("data_worker_usage")
data class DataWorkerUsage(
  @field:Id
  var organizationId: UUID,
  @field:Id
  var workspaceId: UUID,
  @field:Id
  var dataplaneGroupId: UUID,
  var sourceCpuRequest: Double,
  var destinationCpuRequest: Double,
  var orchestratorCpuRequest: Double,
  @field:Id
  var bucketStart: OffsetDateTime,
  var maxSourceCpuRequest: Double,
  var maxDestinationCpuRequest: Double,
  var maxOrchestratorCpuRequest: Double,
  var createdAt: OffsetDateTime,
) {
  fun calculateDataWorkers(): Double {
    val resources = maxSourceCpuRequest + maxDestinationCpuRequest + maxOrchestratorCpuRequest
    val dataWorkers = resources / DATA_WORKER_CPU_DIVISOR
    return dataWorkers
  }

  companion object {
    private const val DATA_WORKER_CPU_DIVISOR = 8
  }
}
