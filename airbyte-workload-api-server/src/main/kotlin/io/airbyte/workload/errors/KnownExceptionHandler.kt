package io.airbyte.workload.errors

import io.airbyte.commons.json.Jsons
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
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
    return HttpResponse.status<Any>(exception.getHttpCode())
      .body(Jsons.serialize(exception.getInfo()))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
