/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import com.fasterxml.jackson.databind.JsonMappingException
import io.airbyte.commons.server.errors.KnownException
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

/**
 * Invalid json input exception.
 */
@Produces
@Singleton
@Requires(classes = [JsonMappingException::class])
class InvalidJsonInputExceptionHandler : ExceptionHandler<JsonMappingException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>?,
    exception: JsonMappingException,
  ): HttpResponse<*> =
    HttpResponse
      .status<Any>(HttpStatus.UNPROCESSABLE_ENTITY)
      .body(KnownException.infoFromThrowableWithMessage(exception, "Invalid json input. ${exception.message} ${exception.originalMessage}"))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
}
