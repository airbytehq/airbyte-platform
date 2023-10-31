package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty

data class WorkloadLabel(
  @ApiModelProperty(required = true)
  var key: String = "",
  @ApiModelProperty(required = true)
  var value: String = "",
)
