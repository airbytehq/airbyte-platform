/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import com.fasterxml.jackson.core.JsonParseException
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.KnownException
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
 * Invalid json input exception.
 */
@Produces
@Singleton
@Requires(classes = [JsonParseException::class])
class InvalidJsonExceptionHandler : ExceptionHandler<JsonParseException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: JsonParseException,
  ): HttpResponse<*> {
    val errorId = UUID.randomUUID()
    // Log full error details
    log.error {
      "Invalid json exception [errorId: $errorId]: ${KnownException.infoFromThrowableWithMessageAndStackTrace(
        exception,
        "Invalid json. ${exception.message} ${exception.originalMessage}",
      )}"
    }

    // Return only errorId in response
    return HttpResponse
      .status<Any>(HttpStatus.UNPROCESSABLE_ENTITY)
      .body(Jsons.serialize(mapOf("errorId" to errorId)))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
