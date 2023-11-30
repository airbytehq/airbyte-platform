package io.airbyte.workload.api.domain

import io.airbyte.workload.api.domain.Constants.Companion.DEFAULT_GEOGRAPHY
import io.swagger.v3.oas.annotations.media.Schema

data class Workload(
  @Schema(required = true)
  var id: String = "",
  var dataplaneId: String? = null,
  var status: WorkloadStatus? = null,
  var labels: MutableList<WorkloadLabel> = mutableListOf(),
  var inputPayload: String = "",
  var logPath: String = "",
  var geography: String = DEFAULT_GEOGRAPHY,
)
