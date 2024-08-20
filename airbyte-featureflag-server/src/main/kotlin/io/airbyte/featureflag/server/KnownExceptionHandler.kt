package io.airbyte.featureflag.server

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Produces
@Singleton
class KnownExceptionHandler : ExceptionHandler<KnownException, HttpResponse<String>> {
  override fun handle(
    request: HttpRequest<Any>,
    exception: KnownException,
  ): HttpResponse<String> {
    return HttpResponse.status<Any>(exception.httpCode).body(exception.message)
  }
}
