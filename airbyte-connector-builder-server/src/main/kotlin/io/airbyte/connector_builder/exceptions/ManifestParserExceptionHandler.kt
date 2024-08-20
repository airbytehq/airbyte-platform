/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.exceptions

import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

/**
 * Custom Micronaut exception handler for the [ManifestParserException].
 */
@Produces
@Singleton
@Requires(classes = [ManifestParserException::class])
class ManifestParserExceptionHandler :
  ExceptionHandler<ManifestParserException?, HttpResponse<*>?> {
  val helper: ExceptionHelper = ExceptionHelper()

  override fun handle(
    request: HttpRequest<*>?,
    exception: ManifestParserException?,
  ): HttpResponse<*>? {
    return helper.handle(request, exception)
  }
}
