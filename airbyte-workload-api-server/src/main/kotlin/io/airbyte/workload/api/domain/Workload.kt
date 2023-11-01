package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty
import java.time.OffsetDateTime

data class Workload(
  @ApiModelProperty(required = true)
  var id: String = "",
  var dataplaneId: String? = null,
  var status: WorkloadStatus? = null,
  var lastHeartbeatAt: OffsetDateTime? = null,
  var labels: MutableList<WorkloadLabel>? = null,
)
