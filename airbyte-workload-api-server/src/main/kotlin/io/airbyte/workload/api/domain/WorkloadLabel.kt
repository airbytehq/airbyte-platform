package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadLabel(
  @Schema(required = true)
  var key: String = "",
  @Schema(required = true)
  var value: String = "",
)
