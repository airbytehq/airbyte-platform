/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.AirbyteApiInterceptor
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.lang.Exception
import java.time.Duration

@Factory
class ApiClientSupportFactory {
  companion object {
    private val clientRetryExceptions: List<Class<out Exception>> =
      listOf(
        IllegalStateException::class.java,
        IOException::class.java,
        UnsupportedOperationException::class.java,
        ClientException::class.java,
        ServerException::class.java,
      )
  }

  @Singleton
  @Named("airbyteApiClientRetryPolicy")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiRetryPolicy(
    @Value("\${airbyte.internal-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.internal-api.retries.max:5}") maxRetries: Int,
    @Value("\${airbyte.internal-api.jitter-factor:.25}") jitterFactor: Double,
    meterRegistry: MeterRegistry?,
  ): RetryPolicy<Response> {
    return generateDefaultRetryPolicy(
      retryDelaySeconds = retryDelaySeconds,
      jitterFactor = jitterFactor,
      maxRetries = maxRetries,
      meterRegistry = meterRegistry,
      metricPrefix = "api-client",
      clientRetryExceptions = clientRetryExceptions,
    )
  }

  @Singleton
  @Named("airbyteApiOkHttpClient")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiOkHttpClient(
    @Value("\${airbyte.internal-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.internal-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    interceptors: List<AirbyteApiInterceptor>,
  ): OkHttpClient {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    interceptors.forEach(builder::addInterceptor)
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }
}
