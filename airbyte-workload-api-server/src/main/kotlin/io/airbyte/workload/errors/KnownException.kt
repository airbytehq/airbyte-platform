package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

abstract class KnownException(message: String?) : Throwable(message) {
  abstract fun getHttpCode(): HttpStatus
}
