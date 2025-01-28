/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers;

import io.airbyte.commons.json.Jsons;
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
 * Known Exception (i.e. an exception that we could anticipate and want to format nicely in the api
 * response).
 */
@Produces
@Singleton
@Requires(classes = KnownException.class)
public class KnownExceptionHandler implements ExceptionHandler<KnownException, HttpResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotFoundExceptionHandler.class);

  @Override
  public HttpResponse handle(final HttpRequest request, final KnownException exception) {
    // Print this info in the logs, but don't send in the request.
    LOGGER.error("Known Exception: {}", exception.getKnownExceptionInfoWithStackTrace());
    return HttpResponse.status(HttpStatus.valueOf(exception.getHttpCode()))
        .body(Jsons.serialize(exception.getKnownExceptionInfo()))
        .contentType(MediaType.APPLICATION_JSON_TYPE);
  }

}
