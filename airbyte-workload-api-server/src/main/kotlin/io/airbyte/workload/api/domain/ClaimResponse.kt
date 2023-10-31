package io.airbyte.workload.api.domain

import io.swagger.annotations.ApiModelProperty

data class ClaimResponse(
  @ApiModelProperty(required = true, value = "Has been claimed")
  var claimed: Boolean = false,
)
