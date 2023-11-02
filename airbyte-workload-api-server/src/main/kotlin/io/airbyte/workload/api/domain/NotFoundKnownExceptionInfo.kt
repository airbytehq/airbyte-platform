package io.airbyte.workload.api.domain

import com.fasterxml.jackson.annotation.JsonTypeName
import io.swagger.v3.oas.annotations.media.Schema

@JsonTypeName("NotFoundKnownExceptionInfo")
data class NotFoundKnownExceptionInfo(
  var id: String? = null,
  @Schema(required = true)
  var message: String = "",
  var exceptionClassName: String? = null,
  var exceptionStack: MutableList<String>? = null,
  var rootCauseExceptionClassName: String? = null,
  var rootCauseExceptionStack: MutableList<String>? = null,
)
