/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.auth.AccessTokenInterceptor
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.api.client.interceptor.AirbyteVersionInterceptor
import io.airbyte.api.client.interceptor.ThrowOn5xxInterceptor
import io.airbyte.api.client.interceptor.UserAgentInterceptor
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.util.concurrent.TimeUnit

@Factory
class ApiClientSupportFactory(
  private val micronautApplicationConfiguration: ApplicationConfiguration,
  private val config: AirbyteInternalApiClientConfig,
  private val metricClient: MetricClient,
) {
  companion object {
    fun newAccessTokenInterceptorFromConfig(config: AirbyteInternalApiClientConfig) =
      AccessTokenInterceptor(
        clientId = config.auth.clientId,
        clientSecret = config.auth.clientSecret,
        tokenEndpoint = config.auth.tokenEndpoint,
        httpClient =
          OkHttpClient
            .Builder()
            .readTimeout(config.readTimeoutSeconds, TimeUnit.SECONDS)
            .connectTimeout(config.connectTimeoutSeconds, TimeUnit.SECONDS)
            .build(),
      )
  }

  @Singleton
  @Requires(property = "airbyte.internal-api.base-path", pattern = ".+")
  fun airbyteInternalApiClient(airbyteConfig: AirbyteConfig): AirbyteApiClient {
    val httpClient =
      OkHttpClient
        .Builder()
        .apply {
          if (config.throwsOn5xx) {
            addInterceptor(ThrowOn5xxInterceptor())
          }

          when (config.auth.type) {
            AirbyteInternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN -> {
              addInterceptor(newAccessTokenInterceptorFromConfig(config))
            }
            AirbyteInternalApiClientConfig.AuthType.INTERNAL_CLIENT_TOKEN -> {
              addInterceptor(
                InternalClientTokenInterceptor(
                  micronautApplicationConfiguration.name.get(),
                  config.auth.signatureSecret,
                ),
              )
            }
          }

          addInterceptor(UserAgentInterceptor(micronautApplicationConfiguration.name.get()))
          addInterceptor(AirbyteVersionInterceptor(airbyteConfig.version))
          readTimeout(config.readTimeoutSeconds, TimeUnit.SECONDS)
          connectTimeout(config.connectTimeoutSeconds, TimeUnit.SECONDS)
        }.build()

    val retryPolicy =
      generateDefaultRetryPolicy(
        retryDelaySeconds = config.retries.delaySeconds,
        jitterFactor = config.retries.jitterFactor,
        maxRetries = config.retries.max,
        metricClient = metricClient,
        clientApiType = ClientApiType.SERVER,
        clientRetryExceptions =
          listOf(
            IllegalStateException::class.java,
            IOException::class.java,
            UnsupportedOperationException::class.java,
            ClientException::class.java,
            ServerException::class.java,
          ),
      )

    return AirbyteApiClient(config.basePath, retryPolicy, httpClient)
  }
}
