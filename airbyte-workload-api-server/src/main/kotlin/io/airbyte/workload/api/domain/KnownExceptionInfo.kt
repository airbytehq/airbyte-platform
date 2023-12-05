package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class KnownExceptionInfo(
  @Schema(required = true)
  var message: String = "",
)
