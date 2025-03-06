/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import io.airbyte.api.client.auth.AirbyteApiInterceptor
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Response
import java.io.IOException

@Singleton
@Named("throwOn5xxInterceptor")
@Requires(property = "airbyte.internal.throws-on-5xx", value = "true", defaultValue = "true")
class ThrowOn5xxInterceptor : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val response = chain.proceed(chain.request())
    if (response.code >= HttpStatus.INTERNAL_SERVER_ERROR.code) {
      throw IOException("HTTP error: ${response.code} ${response.message}")
    }
    return response
  }
}
