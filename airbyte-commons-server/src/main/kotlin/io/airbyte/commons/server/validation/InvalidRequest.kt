package io.airbyte.commons.server.validation

import io.micronaut.http.HttpStatus
import io.micronaut.http.exceptions.HttpStatusException

/**
 * Struct representing an invalid request. Add metadata here as necessary.
 */
class InvalidRequest(message: String?) :
  HttpStatusException(HttpStatus.UNPROCESSABLE_ENTITY, message)
