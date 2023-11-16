package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class Workload(
  @Schema(required = true)
  var id: String = "",
  var dataplaneId: String? = null,
  var status: WorkloadStatus? = null,
  var labels: MutableList<WorkloadLabel> = mutableListOf(),
  var inputPayload: String = "",
  var logPath: String = "",
)
