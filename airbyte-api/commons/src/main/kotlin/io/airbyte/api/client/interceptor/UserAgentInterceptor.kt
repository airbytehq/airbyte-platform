/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import com.google.common.base.CaseFormat
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
      val value = CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, userAgent)
      builder.addHeader(HttpHeaders.USER_AGENT, value)
    }

    return chain.proceed(builder.build())
  }
}
