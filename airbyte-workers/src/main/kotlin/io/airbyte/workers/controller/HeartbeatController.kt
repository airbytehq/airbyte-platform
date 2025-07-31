/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.controller

import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Options
import io.micronaut.http.annotation.Post

/**
 * Heartbeat controller.
 */
@Controller("/")
class HeartbeatController {
  /**
   * Heartbeat.
   *
   * @return ok heartbeat
   */
  @Get(produces = [MediaType.APPLICATION_JSON])
  @Post(produces = [MediaType.APPLICATION_JSON])
  fun heartbeat(): HttpResponse<Map<String, Boolean>> {
    val response = HttpResponse.ok(DEFAULT_RESPONSE_BODY)
    addCorsHeaders(response)
    return response
  }

  /**
   * Return okay for options.
   *
   * @return ok heartbeat
   */
  @Options
  fun emptyHeartbeat(): HttpResponse<Map<String, Boolean>> {
    val response = HttpResponse.ok<Map<String, Boolean>>()
    addCorsHeaders(response)
    return response
  }

  private fun addCorsHeaders(response: MutableHttpResponse<*>) {
    for ((key, value) in CORS_FILTER_MAP) {
      response.header(key, value)
    }
  }

  companion object {
    private val CORS_FILTER_MAP: Map<String, String> =
      mapOf(
        HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN to "*",
        HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS to "Origin, Content-Type, Accept, Content-Encoding",
        HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS to "GET, POST, PUT, DELETE, OPTIONS, HEAD",
      )

    private val DEFAULT_RESPONSE_BODY: Map<String, Boolean> = mapOf("up" to true)
  }
}
