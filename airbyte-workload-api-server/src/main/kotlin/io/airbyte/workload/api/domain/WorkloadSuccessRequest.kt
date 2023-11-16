package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class WorkloadSuccessRequest(
  @Schema(required = true)
  var workloadId: String = "",
)
