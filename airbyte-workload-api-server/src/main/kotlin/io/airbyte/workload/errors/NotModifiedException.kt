package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class NotModifiedException(message: String?) : KnownException(message) {
  override fun getHttpCode(): HttpStatus {
    return HttpStatus.NOT_MODIFIED
  }
}
