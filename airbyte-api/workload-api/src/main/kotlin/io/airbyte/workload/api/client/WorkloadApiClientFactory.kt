/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.api.client.ApiException
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.api.client.config.ApiClientSupportFactory
import io.airbyte.api.client.config.InternalApiClientConfig
import io.airbyte.api.client.interceptor.UserAgentInterceptor
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.metrics.MetricClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okio.IOException
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.create
import java.time.Duration
import kotlin.time.Duration.Companion.seconds

@Factory
@Requires(property = "airbyte.workload-api.base-path")
class WorkloadApiClientFactory(
  @Property(name = "micronaut.application.name") private val applicationName: String,
  @Property(name = "airbyte.workload-api.base-path") private val basePath: String,
  @Property(name = "airbyte.workload-api.retries.delay-seconds") private val retryDelaySeconds: Long = 2,
  @Property(name = "airbyte.workload-api.retries.max") private val maxRetries: Int = 5,
  @Property(name = "airbyte.workload-api.jitter-factor") private val jitterFactor: Double = .25,
  @Property(name = "airbyte.workload-api.connect-timeout-seconds") private val connectTimeoutSeconds: Long,
  @Property(name = "airbyte.workload-api.read-timeout-seconds") private val readTimeoutSeconds: Long,
  private val metricClient: MetricClient,
  private val config: InternalApiClientConfig,
) {
  @Singleton
  fun workloadApiClient(): WorkloadApiClient {
    val retryConfig =
      RetryPolicyConfig(
        delay = retryDelaySeconds.seconds,
        maxRetries = maxRetries,
        jitterFactor = jitterFactor,
        exceptions =
          listOf(
            IllegalArgumentException::class.java,
            IOException::class.java,
            UnsupportedOperationException::class.java,
            ApiException::class.java,
          ),
      )

    val httpClient =
      OkHttpClient
        .Builder()
        .apply {
          // If dataplane client credentials are available, use those, otherwise fall back on the
          // workload bearer token. This is still necessary for non-dataplane applications that call the
          // workload API, like the airbyte-cron and airbyte-worker.
          if (config.auth.type == InternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN) {
            addInterceptor(ApiClientSupportFactory.newAccessTokenInterceptorFromConfig(config))
          } else {
            if (config.auth.signatureSecret.isNullOrBlank()) {
              throw Exception("setting up internal auth interceptor: signatureSecret is null or blank")
            }
            addInterceptor(InternalClientTokenInterceptor(applicationName, config.auth.signatureSecret))
          }

          addInterceptor(UserAgentInterceptor(applicationName))
          readTimeout(Duration.ofSeconds(readTimeoutSeconds))
          connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
        }.build()

    val reactorApi =
      Retrofit
        .Builder()
        .client(httpClient)
        .baseUrl("$basePath/api/v1/workload/")
        // TODO(cole): inject mapper once the mapper has been updated to be injectable
        .addConverterFactory(JacksonConverterFactory.create(MoreMappers.initMapper()))
        .build()
        .create<WorkloadApi>()

    return WorkloadApiClient(
      metricClient = metricClient,
      api = reactorApi,
      retryConfig = retryConfig,
    )
  }
}
