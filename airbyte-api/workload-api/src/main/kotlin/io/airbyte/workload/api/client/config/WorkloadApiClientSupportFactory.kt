/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client.config

import io.airbyte.api.client.UserAgentInterceptor
import io.airbyte.api.client.auth.StaticTokenInterceptor
import io.airbyte.api.client.config.ApiClientSupportFactory
import io.airbyte.api.client.config.ClientApiType
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.api.client.config.InternalApiClientConfig
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.client.WorkloadApiClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import java.io.IOException
import java.time.Duration
import java.util.Base64
import io.airbyte.workload.api.client.generated.infrastructure.ClientException as WorkloadApiClientException
import io.airbyte.workload.api.client.generated.infrastructure.ServerException as WorkloadApiServerException

@Factory
@Requires(property = "airbyte.workload-api.base-path")
class WorkloadApiClientSupportFactory(
  @Property(name = "micronaut.application.name") private val applicationName: String,
  @Property(name = "airbyte.workload-api.base-path") private val basePath: String,
  @Property(name = "airbyte.workload-api.bearer-token") private val bearerToken: String,
  @Property(name = "airbyte.workload-api.retries.delay-seconds") private val retryDelaySeconds: Long = 2,
  @Property(name = "airbyte.workload-api.retries.max") private val maxRetries: Int = 5,
  @Property(name = "airbyte.workload-api.jitter-factor") private val jitterFactor: Double = .25,
  @Property(name = "airbyte.workload-api.connect-timeout-seconds") private val connectTimeoutSeconds: Long,
  @Property(name = "airbyte.workload-api.read-timeout-seconds") private val readTimeoutSeconds: Long,
  private val config: InternalApiClientConfig,
  private val metricClient: MetricClient,
) {
  @Singleton
  fun workloadApiClient(): WorkloadApiClient {
    val retryPolicy =
      generateDefaultRetryPolicy(
        retryDelaySeconds = retryDelaySeconds,
        jitterFactor = jitterFactor,
        maxRetries = maxRetries,
        metricClient = metricClient,
        clientApiType = ClientApiType.WORKLOAD,
        clientRetryExceptions =
          listOf(
            IllegalStateException::class.java,
            IOException::class.java,
            UnsupportedOperationException::class.java,
            WorkloadApiClientException::class.java,
            WorkloadApiServerException::class.java,
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
            val encodedBearerToken = Base64.getEncoder().encodeToString(bearerToken.toByteArray())
            addInterceptor(StaticTokenInterceptor(encodedBearerToken))
          }

          addInterceptor(UserAgentInterceptor(applicationName))
          readTimeout(Duration.ofSeconds(readTimeoutSeconds))
          connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
        }.build()
    return WorkloadApiClient(basePath, retryPolicy, httpClient)
  }
}
