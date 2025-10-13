/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.IdNotFoundKnownException
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
 * Handles a missing id.
 */
@Produces
@Singleton
@Requires(classes = [IdNotFoundKnownException::class])
class IdNotFoundExceptionHandler : ExceptionHandler<IdNotFoundKnownException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: IdNotFoundKnownException,
  ): HttpResponse<*> {
    val errorId = UUID.randomUUID()
    // Log full error details
    log.error { "Id not found exception [errorId: $errorId]: ${exception.getNotFoundKnownExceptionInfoWithStackTrace()}" }

    // Return only errorId in response
    return HttpResponse
      .status<Any>(HttpStatus.NOT_FOUND)
      .body(Jsons.serialize(mapOf("errorId" to errorId)))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
