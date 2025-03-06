/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Handles unprocessable content exceptions.
 */
@Produces
@Singleton
public class UnprocessableEntityExceptionHandler implements ExceptionHandler<UnprocessableEntityException, HttpResponse> {

  @Override
  public HttpResponse handle(final HttpRequest request, final UnprocessableEntityException exception) {
    return HttpResponse.status(HttpStatus.valueOf(exception.getHttpCode()))
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }

}
