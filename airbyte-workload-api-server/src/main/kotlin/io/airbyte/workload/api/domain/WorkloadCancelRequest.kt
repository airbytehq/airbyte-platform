package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadCancelRequest(
  @Schema(required = true)
  var workloadId: String = "",
  @Schema(required = true)
  var reason: String = "",
  @Schema(required = true)
  var source: String = "",
)
