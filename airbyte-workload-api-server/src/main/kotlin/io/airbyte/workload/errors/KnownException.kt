package io.airbyte.workload.errors

import io.airbyte.workload.api.domain.KnownExceptionInfo
import io.micronaut.http.HttpStatus

abstract class KnownException(message: String?) : Throwable(message) {
  abstract fun getHttpCode(): HttpStatus

  fun getInfo(): KnownExceptionInfo {
    return KnownExceptionInfo(this.message ?: "")
  }
}
