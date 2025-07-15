/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

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

private val log = KotlinLogging.logger {}

/**
 * Known Exception (i.e. an exception that we could expect and want to format nicely in the api response).
 */
@Produces
@Singleton
@Requires(classes = [KnownException::class])
class KnownExceptionHandler : ExceptionHandler<KnownException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: KnownException,
  ): HttpResponse<*> {
    // Print this info in the logs, but don't send in the request.
    log.error { "Known Exception: ${exception.getKnownExceptionInfoWithStackTrace()}" }
    return HttpResponse
      .status<Any>(HttpStatus.valueOf(exception.getHttpCode()))
      .body(Jsons.serialize(exception.getKnownExceptionInfo()))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
