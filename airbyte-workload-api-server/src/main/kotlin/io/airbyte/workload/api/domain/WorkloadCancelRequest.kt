package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadCancelRequest(
  @Schema(required = true)
  var workload: MutableList<String> = ArrayList(),
  @Schema(required = true)
  var reason: String = "",
  @Schema(required = true)
  var source: String = "",
)
