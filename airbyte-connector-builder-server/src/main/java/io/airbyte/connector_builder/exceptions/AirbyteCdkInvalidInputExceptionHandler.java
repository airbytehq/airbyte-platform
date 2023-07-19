/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.HashMap;

/**
 * Custom Micronaut exception handler for the {@link AirbyteCdkInvalidInputException}.
 */
@Produces
@Singleton
@Requires(classes = AirbyteCdkInvalidInputException.class)
public class AirbyteCdkInvalidInputExceptionHandler implements ExceptionHandler<AirbyteCdkInvalidInputException, HttpResponse> {

  final ExceptionHelper helper = new ExceptionHelper();

  @Override
  public HttpResponse handle(final HttpRequest request, final AirbyteCdkInvalidInputException exception) {
    final HashMap<String, Object> responseBody = new HashMap<>();
    if (exception.trace != null) {
      helper.updateResponseBodyFromTrace(responseBody, exception.trace);
    } else {
      helper.updateResponseBodyFromException(responseBody, exception);
    }
    return HttpResponse.status(HttpStatus.BAD_REQUEST).body(responseBody);
  }

}
