/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton
import java.util.Optional

/**
 * Custom Micronaut exception handler for the [AirbyteCdkInvalidInputException].
 */
@Produces
@Singleton
@Requires(classes = [AirbyteCdkInvalidInputException::class])
class AirbyteCdkInvalidInputExceptionHandler : ExceptionHandler<AirbyteCdkInvalidInputException, HttpResponse<*>> {
  private val helper: ExceptionHelper = ExceptionHelper()

  override fun handle(
    request: HttpRequest<*>,
    exception: AirbyteCdkInvalidInputException,
  ): HttpResponse<*> = helper.handle(request, exception, HttpStatus.BAD_REQUEST, Optional.ofNullable(exception.trace))
}
