/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
  var maxTotalCpuRequest: Double? = null,
  var createdAt: OffsetDateTime,
) {
  fun calculateDataWorkers(): Double {
    // maxTotalCpuRequest is the max of the sum of all three CPU components at a single point in
    // time, so it is preferred when present. Rows written before the column existed have a null
    // value; for those we fall back to summing the per-component maxes, which is the legacy
    // behaviour. That sum is an overestimate, since the individual maxes may have been set by
    // different jobs at different points within the hour.
    val resources =
      maxTotalCpuRequest ?: (maxSourceCpuRequest + maxDestinationCpuRequest + maxOrchestratorCpuRequest)
    val dataWorkers = resources / DATA_WORKER_CPU_DIVISOR
    return dataWorkers
  }

  companion object {
    private const val DATA_WORKER_CPU_DIVISOR = 8
  }
}
