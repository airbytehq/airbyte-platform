package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadCreateRequest(
  @Schema(required = true)
  var workloadId: String = "",
  var labels: List<WorkloadLabel> = listOf(),
  @Schema(required = true)
  var workloadInput: String = "",
  var logPath: String = "",
)
