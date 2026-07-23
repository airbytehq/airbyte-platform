/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.airbyte.commons.text.lowerHyphenToUpperCamel
import io.micronaut.http.HttpHeaders
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

class UserAgentInterceptor(
  private val userAgent: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (originalRequest.header(HttpHeaders.USER_AGENT) == null) {
      val value = userAgent.lowerHyphenToUpperCamel()
      builder.addHeader(HttpHeaders.USER_AGENT, value)
    }

    return chain.proceed(builder.build())
  }
}
