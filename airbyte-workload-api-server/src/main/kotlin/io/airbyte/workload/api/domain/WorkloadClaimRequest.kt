package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty

data class WorkloadClaimRequest(
  @ApiModelProperty(required = true)
  var workloadId: String = "",
  @ApiModelProperty(required = true)
  var dataplaneId: String = "",
)
