/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.util.Optional

/**
 * Adds a custom Airbyte authentication header to requests made by a client.
 */
@Singleton
@Named("airbyteAuthHeaderInterceptor")
class AirbyteAuthHeaderInterceptor(
  private val authHeaders: Optional<AirbyteAuthHeader>,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    authHeaders.ifPresent { h -> builder.addHeader(h.getHeaderName(), h.getHeaderValue()) }

    return chain.proceed(builder.build())
  }
}
