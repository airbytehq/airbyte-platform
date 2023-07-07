/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper for formatting exception responses.
 */
public class ExceptionHelper {

  /**
   * Format exception response from AirbyteTraceMessage.
   */
  public void updateResponseBodyFromTrace(
                                          final Map<String, Object> responseBody,
                                          final AirbyteTraceMessage trace) {
    responseBody.put("message", trace.getError().getMessage());
    responseBody.put("exceptionClassName", trace.getClass());
    responseBody.put("exceptionStack", trace.getError().getStackTrace());
  }

  /**
   * Format exception response from Exceptions.
   */
  public void updateResponseBodyFromException(
                                              final Map<String, Object> responseBody,
                                              final Exception exception) {
    responseBody.put("message", exception.getMessage());
    responseBody.put("exceptionClassName", exception.getClass());
    responseBody.put("exceptionStack", exception.getStackTrace());
  }

  /**
   * Helper for handling the Internal Server Error response.
   */
  public HttpResponse handle(final HttpRequest request, final Exception exception) {
    final Map<String, Object> responseBody = new HashMap<>();
    this.updateResponseBodyFromException(responseBody, exception);
    return HttpResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).body(responseBody);
  }

}
