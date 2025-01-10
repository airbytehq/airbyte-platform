/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
 * Custom Micronaut exception handler for the [ContributionException].
 */
@Produces
@Singleton
@Requires(classes = [ContributionException::class])
class ContributionExceptionHandler :
  ExceptionHandler<ContributionException?, HttpResponse<*>?> {
  val helper: ExceptionHelper = ExceptionHelper()

  override fun handle(
    request: HttpRequest<*>?,
    exception: ContributionException?,
  ): HttpResponse<*>? {
    return helper.handle(request, exception, exception?.httpStatus)
  }
}
