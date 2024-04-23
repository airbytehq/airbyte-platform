/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import com.google.common.base.CaseFormat
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

private val logger = KotlinLogging.logger {}

fun formatUserAgent(userAgent: String): String {
  return CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, userAgent)
}

@Singleton
class InternalApiAuthenticationInterceptor(
  @Value("\${airbyte.internal-api.auth-header.name}") private val authHeaderName: String,
  @Value("\${airbyte.internal-api.auth-header.value}") private val authHeaderValue: String,
  @Value("\${micronaut.application.name}") private val userAgent: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (originalRequest.header(HttpHeaders.USER_AGENT) == null) {
      builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(userAgent))
    }

    if (authHeaderName.isNotBlank() && authHeaderValue.isNotBlank()) {
      logger.debug { "Adding authorization header..." }
      builder.addHeader(authHeaderName, authHeaderValue)
    } else {
      logger.debug { "Bearer token not provided." }
    }

    return chain.proceed(builder.build())
  }
}
