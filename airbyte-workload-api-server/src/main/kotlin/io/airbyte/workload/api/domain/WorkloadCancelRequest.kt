package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty

data class WorkloadCancelRequest(
  @ApiModelProperty(required = true)
  var workload: MutableList<String> = ArrayList(),
  @ApiModelProperty(required = true, value = "Reason for the cancellation")
  var reason: String = "",
  @ApiModelProperty(required = true, value = "Origin of the cancellation")
  var source: String = "",
)
