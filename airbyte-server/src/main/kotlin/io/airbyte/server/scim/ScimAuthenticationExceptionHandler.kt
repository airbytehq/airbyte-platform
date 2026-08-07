/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimError
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Produces
@Singleton
@Requires(classes = [ScimAuthenticationException::class])
class ScimAuthenticationExceptionHandler : ExceptionHandler<ScimAuthenticationException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: ScimAuthenticationException,
  ): HttpResponse<*> =
    HttpResponse
      .status<ScimError>(HttpStatus.UNAUTHORIZED)
      .contentType(SCIM_MEDIA_TYPE)
      .header(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
      .body(
        scimError(
          status = HttpStatus.UNAUTHORIZED,
          detail = "Invalid bearer token",
        ),
      )
}
