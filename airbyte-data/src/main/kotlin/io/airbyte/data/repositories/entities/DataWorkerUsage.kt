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
  var jobId: Long,
  var organizationId: UUID,
  var workspaceId: UUID,
  var dataplaneGroupId: UUID,
  var sourceCpuRequest: Double,
  var destinationCpuRequest: Double,
  var orchestratorCpuRequest: Double,
  var jobStart: OffsetDateTime,
  var jobEnd: OffsetDateTime?,
  var createdAt: OffsetDateTime,
) {
  fun calculateDataWorkers(): Double {
    val resources = sourceCpuRequest + destinationCpuRequest + orchestratorCpuRequest
    val dataWorkers = resources / DATA_WORKER_CPU_DIVISOR
    return dataWorkers
  }

  companion object {
    private const val DATA_WORKER_CPU_DIVISOR = 8
  }
}
