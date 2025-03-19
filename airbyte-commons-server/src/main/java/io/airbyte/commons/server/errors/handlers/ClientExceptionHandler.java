/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers;

import io.airbyte.commons.json.Jsons;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles unprocessable content exceptions.
 */
@Produces
@Singleton
public class ClientExceptionHandler implements ExceptionHandler<ClientException, HttpResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientExceptionHandler.class);

  @Override
  public HttpResponse handle(final HttpRequest request, final ClientException exception) {
    return HttpResponse.status(HttpStatus.valueOf(exception.getStatusCode()))
        .body(Jsons.serialize(new MessageObject(exception.getMessage())))
        .contentType(MediaType.APPLICATION_JSON);
  }

  private record MessageObject(String message) {}

}
