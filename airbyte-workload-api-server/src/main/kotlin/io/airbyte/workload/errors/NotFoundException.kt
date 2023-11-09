package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class NotFoundException(message: String?) : KnownException(message) {
  override fun getHttpCode(): HttpStatus {
    return HttpStatus.NOT_FOUND
  }
}
