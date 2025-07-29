/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

/**
 * StaticTokenInterceptor sets the "Authorization: Bearer $token"
 * header on all requests, where "$token" is a single, static (unchanging) value.
 */
class StaticTokenInterceptor(
  private val token: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    builder.addHeader("Authorization", "Bearer $token")

    return chain.proceed(builder.build())
  }
}
