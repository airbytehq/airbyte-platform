/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.server.errors.IdNotFoundKnownException
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
import jakarta.ws.rs.NotFoundException

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
    val idnf = IdNotFoundKnownException("Object not found. ${exception.message}", exception)
    log.error { "Not found exception ${idnf.getNotFoundKnownExceptionInfo()}" }

    return HttpResponse
      .status<Any>(HttpStatus.NOT_FOUND)
      .body(KnownException.infoFromThrowableWithMessage(exception, "Internal Server Error: " + exception.message))
      .contentType(MediaType.APPLICATION_JSON)
  }
}
