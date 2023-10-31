package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty

class WorkloadHeartbeatRequest(
  @ApiModelProperty(required = true)
  var workloadId: String = "",
)
