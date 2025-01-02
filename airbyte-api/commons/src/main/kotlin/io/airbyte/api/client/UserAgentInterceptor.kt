/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import com.google.common.base.CaseFormat
import io.airbyte.api.client.auth.AirbyteApiInterceptor
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

fun formatUserAgent(userAgent: String): String {
  return CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, userAgent)
}

@Singleton
@Named("userAgentInterceptor")
class UserAgentInterceptor(
  @Value("\${micronaut.application.name}") private val userAgent: String,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (originalRequest.header(HttpHeaders.USER_AGENT) == null) {
      builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(userAgent))
    }

    return chain.proceed(builder.build())
  }
}
