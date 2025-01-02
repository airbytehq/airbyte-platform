/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.UserAgentInterceptor
import io.airbyte.api.client.auth.AirbyteAuthHeaderInterceptor
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.workload.api.client.auth.WorkloadApiAuthenticationInterceptor
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import java.io.IOException
import java.lang.Exception
import java.time.Duration
import io.airbyte.workload.api.client.generated.infrastructure.ClientException as WorkloadApiClientException
import io.airbyte.workload.api.client.generated.infrastructure.ServerException as WorkloadApiServerException

@Factory
class WorkloadApiClientSupportFactory {
  companion object {
    private val clientRetryExceptions: List<Class<out Exception>> =
      listOf(
        IllegalStateException::class.java,
        IOException::class.java,
        UnsupportedOperationException::class.java,
        WorkloadApiClientException::class.java,
        WorkloadApiServerException::class.java,
      )
  }

  @Singleton
  @Named("workloadApiClientRetryPolicy")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiRetryPolicy(
    @Value("\${airbyte.workload-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.workload-api.retries.max:5}") maxRetries: Int,
    @Value("\${airbyte.workload-api.jitter-factor:.25}") jitterFactor: Double,
    meterRegistry: MeterRegistry?,
  ): RetryPolicy<Response> {
    return generateDefaultRetryPolicy(
      retryDelaySeconds = retryDelaySeconds,
      jitterFactor = jitterFactor,
      maxRetries = maxRetries,
      meterRegistry = meterRegistry,
      metricPrefix = "workload-api-client",
      clientRetryExceptions = clientRetryExceptions,
    )
  }

  @Singleton
  @Named("workloadApiOkHttpClient")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiOkHttpClient(
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    @Named("workloadApiAuthenticationInterceptor") workloadApiAuthenticationInterceptor: WorkloadApiAuthenticationInterceptor,
    @Named("userAgentInterceptor") userAgentInterceptor: UserAgentInterceptor,
    @Named("airbyteAuthHeaderInterceptor") airbyteAuthHeaderInterceptor: AirbyteAuthHeaderInterceptor,
  ): OkHttpClient {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    builder.addInterceptor(workloadApiAuthenticationInterceptor)
    builder.addInterceptor(airbyteAuthHeaderInterceptor)
    builder.addInterceptor(userAgentInterceptor)
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }
}
