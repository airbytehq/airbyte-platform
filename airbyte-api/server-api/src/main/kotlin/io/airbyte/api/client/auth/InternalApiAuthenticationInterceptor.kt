/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.api.client.config.InternalApiAuthenticationFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.BeanProvider
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

private val logger = KotlinLogging.logger {}

@Singleton
@Named("internalApiAuthenticationInterceptor")
@Requires(InternalApiAuthenticationFactory.INTERNAL_API_AUTH_TOKEN_BEAN_NAME)
class InternalApiAuthenticationInterceptor(
  @Value("\${airbyte.internal-api.auth-header.name}") private val authHeaderName: String,
  @Named(InternalApiAuthenticationFactory.INTERNAL_API_AUTH_TOKEN_BEAN_NAME) private val authHeaderValue: BeanProvider<String>,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    if (authHeaderName.isNotBlank() && authHeaderValue.isPresent) {
      logger.debug { "Adding authorization header..." }
      builder.addHeader(authHeaderName, authHeaderValue.get())
    } else {
      logger.debug { "Authorization header/value not provided." }
    }

    return chain.proceed(builder.build())
  }
}
