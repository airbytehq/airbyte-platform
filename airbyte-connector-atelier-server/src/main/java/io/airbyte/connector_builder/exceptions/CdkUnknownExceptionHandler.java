/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Custom Micronaut exception handler for the {@link CdkUnknownException}.
 */
@Produces
@Singleton
@Requires(classes = CdkUnknownException.class)
public class CdkUnknownExceptionHandler implements ExceptionHandler<CdkUnknownException, HttpResponse> {

  final ExceptionHelper helper = new ExceptionHelper();

  @Override
  public HttpResponse handle(final HttpRequest request, final CdkUnknownException exception) {
    return helper.handle(request, exception);
  }

}
