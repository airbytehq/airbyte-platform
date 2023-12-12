package io.airbyte.workload.api.domain

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.domain.Constants.Companion.DEFAULT_GEOGRAPHY
import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadCreateRequest(
  @Schema(required = true)
  var workloadId: String = "",
  var labels: List<WorkloadLabel> = listOf(),
  @Schema(required = true)
  var workloadInput: String = "",
  var logPath: String = "",
  var geography: String = DEFAULT_GEOGRAPHY,
  var mutexKey: String = "",
  var type: WorkloadType = WorkloadType.SYNC,
)
