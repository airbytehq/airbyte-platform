/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions;

import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.protocol.models.v0.AirbyteTraceMessage;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Helper for formatting exception responses.
 */
public class ExceptionHelper {

  /**
   * Helper for handling the Internal Server Error response.
   */
  public HttpResponse handle(final HttpRequest request, final Exception exception) {
    return handle(request, exception, HttpStatus.INTERNAL_SERVER_ERROR, Optional.empty());
  }

  /**
   * Helper for handling response errors.
   */
  public HttpResponse handle(final HttpRequest request, final Exception exception, final HttpStatus status) {
    return handle(request, exception, status, Optional.empty());
  }

  /**
   * Helper for handling response errors with a trace message.
   */
  public HttpResponse handle(final HttpRequest request,
                             final Exception exception,
                             final HttpStatus status,
                             final Optional<AirbyteTraceMessage> traceMessage) {
    final Map<String, Object> responseBody = new HashMap<>();
    // Add the exception to the root span so datadog detects it as an error.
    ApmTraceUtils.recordErrorOnRootSpan(exception);

    if (traceMessage.isPresent()) {
      this.updateResponseBodyFromTrace(responseBody, traceMessage.get());
    } else {
      this.updateResponseBodyFromException(responseBody, exception);
    }
    return HttpResponse.status(status).body(responseBody);
  }

  /**
   * Format exception response from AirbyteTraceMessage.
   */
  private void updateResponseBodyFromTrace(
                                           final Map<String, Object> responseBody,
                                           final AirbyteTraceMessage trace) {
    responseBody.put("message", trace.getError().getMessage());
    responseBody.put("exceptionClassName", trace.getClass());
    responseBody.put("exceptionStack", trace.getError().getStackTrace());
  }

  /**
   * Format exception response from Exceptions.
   */
  private void updateResponseBodyFromException(
                                               final Map<String, Object> responseBody,
                                               final Exception exception) {
    responseBody.put("message", exception.getMessage());
    responseBody.put("exceptionClassName", exception.getClass());
    responseBody.put("exceptionStack", exception.getStackTrace());
  }

}
