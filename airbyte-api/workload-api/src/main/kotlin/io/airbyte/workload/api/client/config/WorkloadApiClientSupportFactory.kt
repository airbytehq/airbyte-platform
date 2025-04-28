/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.UserAgentInterceptor
import io.airbyte.api.client.auth.AirbyteAuthHeaderInterceptor
import io.airbyte.api.client.auth.DataplaneAccessTokenInterceptor
import io.airbyte.api.client.config.ClientApiType
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.client.auth.WorkloadApiAuthenticationInterceptor
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
    metricClient: MetricClient,
  ): RetryPolicy<Response> =
    generateDefaultRetryPolicy(
      retryDelaySeconds = retryDelaySeconds,
      jitterFactor = jitterFactor,
      maxRetries = maxRetries,
      metricClient = metricClient,
      clientApiType = ClientApiType.WORKLOAD,
      clientRetryExceptions = clientRetryExceptions,
    )

  @Singleton
  @Named("workloadApiOkHttpClient")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiOkHttpClient(
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    @Named("workloadApiAuthenticationInterceptor") workloadApiAuthenticationInterceptor: WorkloadApiAuthenticationInterceptor,
    dataplaneAccessTokenInterceptor: DataplaneAccessTokenInterceptor?,
    @Named("userAgentInterceptor") userAgentInterceptor: UserAgentInterceptor,
    @Named("airbyteAuthHeaderInterceptor") airbyteAuthHeaderInterceptor: AirbyteAuthHeaderInterceptor,
  ): OkHttpClient {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    // If a dataplaneAccessTokenInterceptor is available, use it. Otherwise, fall back on the
    // workloadApiAuthenticationInterceptor which still uses the workload API bearer token from
    // the environment. This is still necessary for non-dataplane applications that call the
    // workload API, like the airbyte-cron.
    if (dataplaneAccessTokenInterceptor != null) {
      builder.addInterceptor(dataplaneAccessTokenInterceptor)
    } else {
      builder.addInterceptor(workloadApiAuthenticationInterceptor)
      builder.addInterceptor(airbyteAuthHeaderInterceptor) // TODO(parker) look into removing this, may not be doing anything?
    }
    builder.addInterceptor(userAgentInterceptor)
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }
}
