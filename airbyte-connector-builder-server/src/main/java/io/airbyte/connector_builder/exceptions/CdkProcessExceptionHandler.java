/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Custom Micronaut exception handler for the {@link CdkProcessException}.
 */
@Produces
@Singleton
@Requires(classes = CdkProcessException.class)
public class CdkProcessExceptionHandler implements ExceptionHandler<CdkProcessException, HttpResponse> {

  final ExceptionHelper helper = new ExceptionHelper();

  @Override
  public HttpResponse handle(final HttpRequest request, final CdkProcessException exception) {
    return helper.handle(request, exception);
  }

}
