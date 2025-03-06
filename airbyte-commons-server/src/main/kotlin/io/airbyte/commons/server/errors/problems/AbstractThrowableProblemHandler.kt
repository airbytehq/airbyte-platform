/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.problems

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.ProblemResponse
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

@Produces
@Singleton
@Requires(classes = [AbstractThrowableProblem::class])
class AbstractThrowableProblemHandler : ExceptionHandler<AbstractThrowableProblem, HttpResponse<*>> {
  companion object {
    private val log = LoggerFactory.getLogger(AbstractThrowableProblemHandler::class.java)
  }

  override fun handle(
    request: HttpRequest<*>?,
    exception: AbstractThrowableProblem?,
  ): HttpResponse<*>? {
    if (exception != null) {
      log.error("Throwable Problem Handler caught exception: ", exception)
      val problem: ProblemResponse = exception.problem

      val status: HttpStatus = HttpStatus.valueOf(problem.status)
      return HttpResponse
        .status<Any>(status)
        .body(Jsons.serialize(problem))
        .contentType(MediaType.APPLICATION_JSON_TYPE)
    }
    return HttpResponse
      .status<Any>(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(Jsons.serialize(mapOf("message" to "Internal Server Error when building response for exception.")))
      .contentType(MediaType.APPLICATION_JSON_TYPE)
  }
}
