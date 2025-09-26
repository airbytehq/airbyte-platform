/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.api.client.ApiException
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.api.client.config.ApiClientSupportFactory
import io.airbyte.api.client.interceptor.AirbyteVersionInterceptor
import io.airbyte.api.client.interceptor.UserAgentInterceptor
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import io.airbyte.micronaut.runtime.AirbyteWorkloadApiClientConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.ApplicationConfiguration
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
  private val micronautApplicationConfiguration: ApplicationConfiguration,
  private val airbyteWorkloadApiClientConfig: AirbyteWorkloadApiClientConfig,
  private val metricClient: MetricClient,
  private val airbyteInternalApiClientConfig: AirbyteInternalApiClientConfig,
) {
  @Singleton
  fun workloadApiClient(airbyteConfig: AirbyteConfig): WorkloadApiClient {
    val retryConfig =
      RetryPolicyConfig(
        delay = airbyteWorkloadApiClientConfig.retries.delaySeconds.seconds,
        maxRetries = airbyteWorkloadApiClientConfig.retries.max,
        jitterFactor = airbyteWorkloadApiClientConfig.retries.jitterFactor,
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
          if (airbyteInternalApiClientConfig.auth.type == AirbyteInternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN) {
            addInterceptor(ApiClientSupportFactory.newAccessTokenInterceptorFromConfig(airbyteInternalApiClientConfig))
          } else {
            if (airbyteInternalApiClientConfig.auth.signatureSecret.isBlank()) {
              throw Exception("setting up internal auth interceptor: signatureSecret is null or blank")
            }
            addInterceptor(
              InternalClientTokenInterceptor(
                subject = micronautApplicationConfiguration.name.get(),
                signatureSecret = airbyteInternalApiClientConfig.auth.signatureSecret,
              ),
            )
          }

          addInterceptor(UserAgentInterceptor(micronautApplicationConfiguration.name.get()))
          addInterceptor(AirbyteVersionInterceptor(airbyteConfig.version))
          readTimeout(Duration.ofSeconds(airbyteWorkloadApiClientConfig.readTimeoutSeconds))
          connectTimeout(Duration.ofSeconds(airbyteWorkloadApiClientConfig.connectTimeoutSeconds))
        }.build()

    val reactorApi =
      Retrofit
        .Builder()
        .client(httpClient)
        .baseUrl("${airbyteWorkloadApiClientConfig.basePath}/api/v1/workload/")
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
