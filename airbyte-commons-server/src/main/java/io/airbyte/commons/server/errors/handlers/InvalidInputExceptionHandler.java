/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers;

import io.airbyte.api.model.generated.InvalidInputExceptionInfo;
import io.airbyte.api.model.generated.InvalidInputProperty;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.validation.exceptions.ConstraintExceptionHandler;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.List;

/**
 * https://www.baeldung.com/jersey-bean-validation#custom-exception-handler. handles exceptions
 * related to the request body not matching the openapi config.
 */
@Produces
@Singleton
@Requires(classes = ConstraintViolationException.class)
@Replaces(ConstraintExceptionHandler.class)
public class InvalidInputExceptionHandler implements ExceptionHandler<ConstraintViolationException, HttpResponse> {

  /**
   * Re-route the invalid input to a meaningful HttpStatus.
   */
  @Override
  public HttpResponse handle(final HttpRequest request, final ConstraintViolationException exception) {
    return HttpResponse.status(HttpStatus.BAD_REQUEST)
        .body(Jsons.serialize(infoFromConstraints(exception)))
        .contentType(MediaType.APPLICATION_JSON_TYPE);
  }

  /**
   * Static factory for invalid input.
   *
   * @param cve exception with invalidity info
   * @return exception
   */
  public static InvalidInputExceptionInfo infoFromConstraints(final ConstraintViolationException cve) {
    final InvalidInputExceptionInfo exceptionInfo = new InvalidInputExceptionInfo()
        .exceptionClassName(cve.getClass().getName())
        .message("Some properties contained invalid input.")
        .exceptionStack(KnownException.getStackTraceAsList(cve));

    final List<InvalidInputProperty> props = new ArrayList<InvalidInputProperty>();
    for (final ConstraintViolation<?> cv : cve.getConstraintViolations()) {
      props.add(new InvalidInputProperty()
          .propertyPath(cv.getPropertyPath().toString())
          .message(cv.getMessage())
          .invalidValue(cv.getInvalidValue() != null ? cv.getInvalidValue().toString() : "null"));
    }
    exceptionInfo.validationErrors(props);
    return exceptionInfo;
  }

}
