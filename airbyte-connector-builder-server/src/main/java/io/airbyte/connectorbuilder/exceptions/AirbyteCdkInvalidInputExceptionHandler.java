/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.Optional;

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
    return this.helper.handle(request, exception, HttpStatus.BAD_REQUEST, Optional.ofNullable(exception.trace));
  }

}
