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
 * Custom Micronaut exception handler for the {@link ConnectorBuilderException}.
 */
@Produces
@Singleton
@Requires(classes = ConnectorBuilderException.class)
public class ConnectorBuilderExceptionHandler implements ExceptionHandler<ConnectorBuilderException, HttpResponse> {

  final ExceptionHelper helper = new ExceptionHelper();

  @Override
  public HttpResponse handle(final HttpRequest request, final ConnectorBuilderException exception) {
    return this.helper.handle(request, exception);
  }

}
