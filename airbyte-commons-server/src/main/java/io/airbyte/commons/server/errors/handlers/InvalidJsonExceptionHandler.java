/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers;

import com.fasterxml.jackson.core.JsonParseException;
import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Invalid json input exception.
 */
@Produces
@Singleton
@Requires(classes = JsonParseException.class)
public class InvalidJsonExceptionHandler implements ExceptionHandler<JsonParseException, HttpResponse> {

  @Override
  public HttpResponse handle(final HttpRequest request, final JsonParseException exception) {
    return HttpResponse.status(HttpStatus.UNPROCESSABLE_ENTITY)
        .body(
            KnownException.infoFromThrowableWithMessage(exception, "Invalid json. " + exception.getMessage() + " " + exception.getOriginalMessage()))
        .contentType(MediaType.APPLICATION_JSON_TYPE);
  }

}
