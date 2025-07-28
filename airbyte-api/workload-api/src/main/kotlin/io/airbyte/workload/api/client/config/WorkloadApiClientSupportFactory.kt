/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.ApiException
import io.airbyte.api.client.auth.DataplaneAccessTokenInterceptor
import io.airbyte.api.client.config.ClientApiType
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetrofitRetryPolicy
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.api.client.interceptor.UserAgentInterceptor
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.WorkloadApiClient
import io.airbyte.workload.api.client.auth.WorkloadApiAuthenticationInterceptor
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.create
import java.io.IOException
import java.lang.Exception
import java.time.Duration

@Factory
class WorkloadApiClientSupportFactory {
  companion object {
    private val clientRetryExceptions: List<Class<out Exception>> =
      listOf(
        IllegalStateException::class.java,
        IOException::class.java,
        UnsupportedOperationException::class.java,
        ApiException::class.java,
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
  @Named("workloadApiClientRetrofitRetryPolicy")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiRetrofitRetryPolicy(
    @Value("\${airbyte.workload-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.workload-api.retries.max:5}") maxRetries: Int,
    @Value("\${airbyte.workload-api.jitter-factor:.25}") jitterFactor: Double,
    metricClient: MetricClient,
  ): RetryPolicy<retrofit2.Response<Any>> =
    generateDefaultRetrofitRetryPolicy(
      retryDelaySeconds = retryDelaySeconds,
      jitterFactor = jitterFactor,
      maxRetries = maxRetries,
      metricClient = metricClient,
      clientApiType = ClientApiType.WORKLOAD,
      clientRetryExceptions = clientRetryExceptions,
    )

  @Singleton
  @Named("workloadApiOkHttpClient")
  @Requires(property = "airbyte.workload-api.base-path", pattern = ".+")
  fun defaultWorkloadApiOkHttpClient(
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    @Named("workloadApiAuthenticationInterceptor") workloadApiAuthenticationInterceptor: WorkloadApiAuthenticationInterceptor,
    dataplaneAccessTokenInterceptor: DataplaneAccessTokenInterceptor?,
    @Named("userAgentInterceptor") userAgentInterceptor: UserAgentInterceptor,
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
    }
    builder.addInterceptor(userAgentInterceptor)
    // uncomment to enable request and response logging
    // builder.addInterceptor(HttpLoggingInterceptor().apply { level = HttpLoggingInterceptor.Level.BODY })
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }

  @Singleton
  @Requires(property = "airbyte.workload-api.base-path", pattern = ".+")
  fun retrofitClient(
    @Named("workloadApiOkHttpClient") okhttpClient: OkHttpClient,
    @Value("\${airbyte.workload-api.base-path}") basePath: String,
  ): WorkloadApiClient {
    val reactor =
      Retrofit
        .Builder()
        .client(okhttpClient)
        .baseUrl("$basePath/api/v1/workload/")
        // TODO(cole): inject mapper once the mapper has been updated to be injectable
        .addConverterFactory(JacksonConverterFactory.create(MoreMappers.initMapper()))
        .build()

    return reactor.create<WorkloadApiClient>()
  }
}
