package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime

data class Workload(
  @Schema(required = true)
  var id: String = "",
  var dataplaneId: String? = null,
  var status: WorkloadStatus? = null,
  var lastHeartbeatAt: OffsetDateTime? = null,
  var labels: MutableList<WorkloadLabel>? = null,
)
