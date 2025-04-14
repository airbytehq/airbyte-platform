/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.AirbyteApiInterceptor
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.metrics.MetricClient
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
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private val CLIENT_RETRY_EXCEPTIONS: List<Class<out Exception>> =
  listOf(
    IllegalStateException::class.java,
    IOException::class.java,
    UnsupportedOperationException::class.java,
    ClientException::class.java,
    ServerException::class.java,
  )

@Factory
class ApiClientSupportFactory {
  @Singleton
  @Named("airbyteApiClientRetryPolicy")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiRetryPolicy(
    @Value("\${airbyte.internal-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.internal-api.retries.max:5}") maxRetries: Int,
    @Value("\${airbyte.internal-api.jitter-factor:.25}") jitterFactor: Double,
    metricClient: MetricClient,
  ): RetryPolicy<Response> =
    generateDefaultRetryPolicy(
      retryDelaySeconds = retryDelaySeconds,
      jitterFactor = jitterFactor,
      maxRetries = maxRetries,
      metricClient = metricClient,
      clientApiType = ClientApiType.SERVER,
      clientRetryExceptions = CLIENT_RETRY_EXCEPTIONS,
    )

  @Singleton
  @Named("airbyteApiOkHttpClient")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiOkHttpClient(
    @Value("\${airbyte.internal-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.internal-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    interceptors: List<AirbyteApiInterceptor>,
  ): OkHttpClient =
    OkHttpClient
      .Builder()
      .apply {
        interceptors.forEach { addInterceptor(it) }
        readTimeout(readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(connectTimeoutSeconds.seconds.toJavaDuration())
      }.build()

  @Singleton
  @Named("airbyteApiOkHttpClientWithoutInterceptors")
  @Requires(property = "airbyte.internal-api.base-path")
  fun airbyteApiOkHttpClientWithoutInterceptors(
    @Value("\${airbyte.internal-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.internal-api.read-timeout-seconds}") readTimeoutSeconds: Long,
  ): OkHttpClient =
    OkHttpClient
      .Builder()
      .apply {
        readTimeout(readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(connectTimeoutSeconds.seconds.toJavaDuration())
      }.build()
}
