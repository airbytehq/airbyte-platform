/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import jakarta.ws.rs.NotFoundException
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Resource not found exception.
 */
@Produces
@Singleton
@Requires(classes = [NotFoundException::class])
class NotFoundExceptionHandler : ExceptionHandler<NotFoundException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: NotFoundException,
  ): HttpResponse<*>? {
    val errorId = UUID.randomUUID()
    val idnf = IdNotFoundKnownException("Object not found. ${exception.message}", exception)
    // Log full error details
    log.error { "Not found exception [errorId: $errorId]: ${idnf.getKnownExceptionInfoWithStackTrace()}" }

    // Return only errorId in response
    return HttpResponse
      .status<Any>(HttpStatus.NOT_FOUND)
      .body(Jsons.serialize(mapOf("errorId" to errorId)))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
