/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.metrics.lib.ApmTraceUtils.recordErrorOnRootSpan
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import java.util.Optional

/**
 * Helper for formatting exception responses.
 */
class ExceptionHelper {
  @Suppress("UNUSED_PARAMETER")
  @JvmOverloads
  fun handle(
    request: HttpRequest<*>,
    exception: Exception,
    status: HttpStatus? = HttpStatus.INTERNAL_SERVER_ERROR,
    traceMessage: Optional<AirbyteTraceMessage> = Optional.empty(),
  ): HttpResponse<*> {
    val responseBody: MutableMap<String, Any?> = HashMap()
    // Add the exception to the root span so datadog detects it as an error.
    recordErrorOnRootSpan(exception)

    if (traceMessage.isPresent) {
      this.updateResponseBodyFromTrace(responseBody, traceMessage.get())
    } else {
      this.updateResponseBodyFromException(responseBody, exception)
    }
    return HttpResponse.status<Any>(status).body<Map<String, Any?>>(responseBody)
  }

  /**
   * Format exception response from AirbyteTraceMessage.
   */
  private fun updateResponseBodyFromTrace(
    responseBody: MutableMap<String, Any?>,
    trace: AirbyteTraceMessage,
  ) {
    responseBody["message"] = trace.error.message
    responseBody["exceptionClassName"] = trace.javaClass
    responseBody["exceptionStack"] = trace.error.stackTrace
  }

  /**
   * Format exception response from Exceptions.
   */
  private fun updateResponseBodyFromException(
    responseBody: MutableMap<String, Any?>,
    exception: Exception,
  ) {
    responseBody["message"] = exception.message
    responseBody["exceptionClassName"] = exception.javaClass
    responseBody["exceptionStack"] = exception.stackTrace
  }
}
