/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers;

import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a missing id.
 */
@Produces
@Singleton
@Requires(classes = IdNotFoundKnownException.class)
public class IdNotFoundExceptionHandler implements ExceptionHandler<IdNotFoundKnownException, HttpResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdNotFoundExceptionHandler.class);

  @Override
  public HttpResponse handle(final HttpRequest request, final IdNotFoundKnownException exception) {
    final IdNotFoundKnownException idnf = new IdNotFoundKnownException("Id not found: " + exception.getMessage(), exception);
    LOGGER.error("Not found exception {}", idnf.getNotFoundKnownExceptionInfo());

    return HttpResponse.status(HttpStatus.NOT_FOUND)
        .body(KnownException.infoFromThrowableWithMessage(exception, "Internal Server Error: " + exception.getMessage()))
        .contentType(MediaType.APPLICATION_JSON);
  }

}
