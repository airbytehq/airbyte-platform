package io.airbyte.workload.errors

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Produces
@Singleton
class KnownExceptionHandler : ExceptionHandler<KnownException, HttpResponse<Any>> {
  override fun handle(
    request: HttpRequest<Any>,
    exception: KnownException,
  ): HttpResponse<Any> {
    return HttpResponse.status(exception.getHttpCode())
  }
}
