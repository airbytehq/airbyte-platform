/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

/**
 * Custom Micronaut exception handler for the [ConnectorBuilderException].
 */
@Produces
@Singleton
@Requires(classes = [ConnectorBuilderException::class])
class ConnectorBuilderExceptionHandler : ExceptionHandler<ConnectorBuilderException, HttpResponse<*>> {
  private val helper: ExceptionHelper = ExceptionHelper()

  override fun handle(
    request: HttpRequest<*>,
    exception: ConnectorBuilderException,
  ): HttpResponse<*> = helper.handle(request, exception)
}
