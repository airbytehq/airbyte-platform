/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

/**
 * Handles unprocessable content exceptions.
 */
@Produces
@Singleton
class UnprocessableEntityExceptionHandler : ExceptionHandler<UnprocessableEntityException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: UnprocessableEntityException,
  ): HttpResponse<*> =
    HttpResponse
      .status<Any>(HttpStatus.valueOf(exception.getHttpCode()))
      .body(exception.message)
      .contentType(MediaType.TEXT_PLAIN)
}
