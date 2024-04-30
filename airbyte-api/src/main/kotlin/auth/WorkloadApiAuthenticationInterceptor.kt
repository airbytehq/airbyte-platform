/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.util.Base64

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadApiAuthenticationInterceptor(
  @Value("\${airbyte.workload-api.bearer-token}") private val bearerToken: String,
  @Value("\${micronaut.application.name}") private val userAgent: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (originalRequest.header(HttpHeaders.USER_AGENT) == null) {
      builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(userAgent))
    }

    if (bearerToken.isNotBlank()) {
      logger.debug { "Adding authorization header..." }
      val encodedBearerToken = Base64.getEncoder().encodeToString(bearerToken.toByteArray())
      builder.addHeader(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $encodedBearerToken")
    } else {
      logger.debug { "Bearer token not provided." }
    }

    return chain.proceed(builder.build())
  }

  companion object {
    const val BEARER_TOKEN_PREFIX = "Bearer"
  }
}
