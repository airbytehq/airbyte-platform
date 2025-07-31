/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.api.model.generated.InvalidInputExceptionInfo
import io.airbyte.api.model.generated.InvalidInputProperty
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.KnownException
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import io.micronaut.validation.exceptions.ConstraintExceptionHandler
import jakarta.inject.Singleton
import jakarta.validation.ConstraintViolationException

/**
 * https://www.baeldung.com/jersey-bean-validation#custom-exception-handler. handles exceptions
 * related to the request body not matching the openapi config.
 */
@Produces
@Singleton
@Requires(classes = [ConstraintViolationException::class])
@Replaces(ConstraintExceptionHandler::class)
class InvalidInputExceptionHandler : ExceptionHandler<ConstraintViolationException, HttpResponse<*>> {
  /**
   * Re-route the invalid input to a meaningful HttpStatus.
   */
  override fun handle(
    request: HttpRequest<*>,
    exception: ConstraintViolationException,
  ): HttpResponse<*> =
    HttpResponse
      .status<Any>(HttpStatus.BAD_REQUEST)
      .body(Jsons.serialize(infoFromConstraints(exception)))
      .contentType(MediaType.APPLICATION_JSON_TYPE)

  companion object {
    /**
     * Static factory for invalid input.
     *
     * @param cve exception with invalidity info
     * @return exception
     */
    fun infoFromConstraints(cve: ConstraintViolationException): InvalidInputExceptionInfo {
      val exceptionInfo =
        InvalidInputExceptionInfo()
          .exceptionClassName(cve.javaClass.getName())
          .message("Some properties contained invalid input.")
          .exceptionStack(KnownException.getStackTraceAsList(cve))

      val props: MutableList<InvalidInputProperty?> = ArrayList<InvalidInputProperty?>()
      for (cv in cve.constraintViolations) {
        props.add(
          InvalidInputProperty()
            .propertyPath(cv.propertyPath.toString())
            .message(cv.message)
            .invalidValue(cv.invalidValue?.toString() ?: "null"),
        )
      }
      exceptionInfo.validationErrors(props)
      return exceptionInfo
    }
  }
}
