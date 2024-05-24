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
import org.slf4j.LoggerFactory

/**
 * Micronaut ExceptionHandler for public API's AbstractThrowableProblem.
 * We're moving towards generated Problems - this class should be removed once the migration is complete.
 * See AbstractThrowableProblemHandler for the new implementation.
 */
@Produces
@Singleton
@Deprecated("AbstractThrowableProblemHandler should be used instead")
@Requires(classes = [AbstractThrowableProblem::class])
class LegacyAbstractThrowableProblemHandler :
  ExceptionHandler<AbstractThrowableProblem, HttpResponse<*>> {
  companion object {
    private val log = LoggerFactory.getLogger(LegacyAbstractThrowableProblemHandler::class.java)
  }

  override fun handle(
    request: HttpRequest<*>?,
    exception: AbstractThrowableProblem?,
  ): HttpResponse<*>? {
    if (exception != null) {
      log.error("Legacy Throwable Problem Handler caught exception: ", exception)
      return HttpResponse.status<Any>(HttpStatus.valueOf(exception.httpCode))
        .body(Jsons.serialize(exception.apiProblemInfo ?: {}))
        .contentType(MediaType.APPLICATION_JSON_TYPE)
    }
    return HttpResponse.status<Any>(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(Jsons.serialize(mapOf("message" to "Internal Server Error when building response for exception.")))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
