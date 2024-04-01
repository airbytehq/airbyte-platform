package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.json.Jsons
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Produces
@Singleton
@Requires(classes = [AbstractThrowableProblem::class])
class AbstractThrowableProblemHandler :
  ExceptionHandler<AbstractThrowableProblem, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>?,
    exception: AbstractThrowableProblem?,
  ): HttpResponse<*>? {
    if (exception != null) {
      return HttpResponse.status<Any>(HttpStatus.valueOf(exception.httpCode))
        .body(Jsons.serialize(exception.apiProblemInfo ?: {}))
        .contentType(MediaType.APPLICATION_JSON_TYPE)
    }
    return HttpResponse.status<Any>(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(Jsons.serialize(mapOf("message" to "Internal Server Error when building response for exception.")))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
