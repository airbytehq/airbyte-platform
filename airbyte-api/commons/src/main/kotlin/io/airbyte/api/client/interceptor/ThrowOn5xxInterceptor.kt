/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.micronaut.http.HttpStatus
import okhttp3.Interceptor
import okhttp3.Response
import java.io.IOException

class ThrowOn5xxInterceptor : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val response = chain.proceed(chain.request())
    if (response.code >= HttpStatus.INTERNAL_SERVER_ERROR.code) {
      throw IOException("HTTP error: ${response.code} ${response.message}")
    }
    return response
  }
}
