/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.util.Optional

/**
 * Adds a custom Airbyte authentication header to requests made by a client.
 */
@Singleton
class AirbyteAuthHeaderInterceptor(
  private val authHeaders: Optional<AirbyteAuthHeader>,
  @Value("\${micronaut.application.name}") private val userAgent: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (originalRequest.header(HttpHeaders.USER_AGENT) == null) {
      builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(userAgent))
    }

    authHeaders.ifPresent { h -> builder.addHeader(h.getHeaderName(), h.getHeaderValue()) }

    return chain.proceed(builder.build())
  }
}
