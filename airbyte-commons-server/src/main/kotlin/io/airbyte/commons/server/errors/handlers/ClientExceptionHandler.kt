/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.KnownException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Handles unprocessable content exceptions.
 */
@Produces
@Singleton
class ClientExceptionHandler : ExceptionHandler<ClientException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: ClientException,
  ): HttpResponse<*> {
    val errorId = UUID.randomUUID()
    // Log full error details
    log.error {
      "Client exception [errorId: $errorId]: ${KnownException.infoFromThrowableWithMessageAndStackTrace(
        exception,
        exception.message ?: "Client exception",
      )}"
    }

    // Return only errorId in response
    return HttpResponse
      .status<Any>(HttpStatus.valueOf(exception.statusCode))
      .body(Jsons.serialize(mapOf("errorId" to errorId)))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
