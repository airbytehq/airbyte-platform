/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.KnownException
import io.airbyte.data.services.impls.keycloak.InvalidClientCredentialsException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Translates a Keycloak client_credentials rejection into an HTTP 401 response.
 */
@Produces
@Singleton
@Requires(classes = [InvalidClientCredentialsException::class])
class InvalidClientCredentialsExceptionHandler : ExceptionHandler<InvalidClientCredentialsException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: InvalidClientCredentialsException,
  ): HttpResponse<*> {
    val errorId = UUID.randomUUID()
    log.error {
      "Invalid client credentials [errorId: $errorId]: ${KnownException.infoFromThrowableWithMessageAndStackTrace(
        exception,
        exception.message ?: "Invalid client credentials",
      )}"
    }

    return HttpResponse
      .status<Any>(HttpStatus.UNAUTHORIZED)
      .body(Jsons.serialize(mapOf("errorId" to errorId)))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
