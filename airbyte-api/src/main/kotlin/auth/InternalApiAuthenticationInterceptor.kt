/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

private val logger = KotlinLogging.logger {}

@Singleton
@Named("internalApiAuthenticationInterceptor")
class InternalApiAuthenticationInterceptor(
  @Value("\${airbyte.internal-api.auth-header.name}") private val authHeaderName: String,
  @Value("\${airbyte.internal-api.auth-header.value}") private val authHeaderValue: String,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (authHeaderName.isNotBlank() && authHeaderValue.isNotBlank()) {
      logger.debug { "Adding authorization header..." }
      builder.addHeader(authHeaderName, authHeaderValue)
    } else {
      logger.debug { "Authorization header/value not provided." }
    }

    return chain.proceed(builder.build())
  }
}
