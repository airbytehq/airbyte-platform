/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.server.errors.KnownException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Unanticipated exception. Treat it like an Internal Server Error.
 */
@Produces
@Singleton
@Requires(classes = [Throwable::class])
@Primary
class UncaughtExceptionHandler : ExceptionHandler<Throwable, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: Throwable,
  ): HttpResponse<*> {
    log.error(exception) { "Uncaught exception" }

    return HttpResponse
      .status<Any>(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(KnownException.infoFromThrowableWithMessage(exception, "Internal Server Error: ${exception.message}"))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
