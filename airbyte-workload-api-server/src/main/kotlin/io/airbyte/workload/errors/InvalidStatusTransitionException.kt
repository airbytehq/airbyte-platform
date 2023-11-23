package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class InvalidStatusTransitionException(message: String?) : KnownException(message) {
  override fun getHttpCode(): HttpStatus {
    return HttpStatus.GONE
  }
}
