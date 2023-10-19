package io.airbyte.api.server.errorHandlers

import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import io.micronaut.problem.violations.ProblemConstraintViolationExceptionHandler
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import javax.validation.ConstraintViolationException

/**
 * https://www.baeldung.com/jersey-bean-validation#custom-exception-handler. handles exceptions
 * related to the request body not matching the openapi config.
 */
@Produces
@Singleton
@Requires(classes = [ConstraintViolationException::class])
@Replaces(
  ProblemConstraintViolationExceptionHandler::class,
)
class ConstraintViolationExceptionHandler :
  ExceptionHandler<ConstraintViolationException?, HttpResponse<*>> {
  companion object {
    val log = LoggerFactory.getLogger(ConstraintViolationExceptionHandler::class.java)
  }

  /**
   * Re-route the invalid input to a meaningful HttpStatus.
   */
  override fun handle(
    request: HttpRequest<*>?,
    exception: ConstraintViolationException?,
  ): HttpResponse<*> {
    log.debug("ConstraintViolationException: {}", exception)
    return HttpResponse.status<Any>(HttpStatus.BAD_REQUEST)
      .body(exception!!.stackTraceToString())
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
