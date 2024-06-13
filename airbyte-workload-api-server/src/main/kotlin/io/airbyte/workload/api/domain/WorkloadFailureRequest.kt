package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadFailureRequest(
  @Schema(required = true)
  var workloadId: String = "",
  var source: String? = null,
  var reason: String? = null,
)
