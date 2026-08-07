/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimError
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Produces
@Singleton
@Requires(classes = [ScimException::class])
class ScimExceptionHandler : ExceptionHandler<ScimException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: ScimException,
  ): HttpResponse<*> =
    HttpResponse
      .status<ScimError>(exception.status)
      .contentType(SCIM_MEDIA_TYPE)
      .body(
        scimError(
          status = exception.status,
          detail = exception.message,
          scimType = exception.scimType,
        ),
      )
}
