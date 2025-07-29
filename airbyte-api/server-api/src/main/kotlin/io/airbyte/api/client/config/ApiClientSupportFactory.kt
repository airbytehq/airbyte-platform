/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.ThrowOn5xxInterceptor
import io.airbyte.api.client.UserAgentInterceptor
import io.airbyte.api.client.auth.AccessTokenInterceptor
import io.airbyte.api.client.auth.KeycloakAccessTokenInterceptor
import io.airbyte.api.client.auth.StaticTokenInterceptor
import io.airbyte.api.client.config.ClientConfigurationSupport.generateDefaultRetryPolicy
import io.airbyte.metrics.MetricClient
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.util.concurrent.TimeUnit

@ConfigurationProperties("airbyte.internal-api")
data class InternalApiClientConfig(
  val basePath: String?,
  val workloadApiHost: String?,
  val connectorBuilderApiHost: String?,
  val connectTimeoutSeconds: Long = 30,
  val readTimeoutSeconds: Long = 600,
  val throwsOn5xx: Boolean = true,
  val retries: RetryConfig,
  val auth: AuthConfig,
) {
  enum class AuthType {
    DATAPLANE_ACCESS_TOKEN,
    KEYCLOAK_ACCESS_TOKEN,
    STATIC_TOKEN,
  }

  @ConfigurationProperties("auth")
  data class AuthConfig(
    val type: AuthType,
    val clientId: String?,
    val clientSecret: String?,
    val tokenEndpoint: String?,
    val token: String?,
  )

  @ConfigurationProperties("retries")
  data class RetryConfig(
    val max: Int = 5,
    val delaySeconds: Long = 2,
    val jitterFactor: Double = .25,
  )
}

@Factory
class ApiClientSupportFactory(
  @Property(name = "micronaut.application.name") private val applicationName: String,
  private val config: InternalApiClientConfig,
  private val keycloakAccessTokenInterceptor: KeycloakAccessTokenInterceptor?,
  private val metricClient: MetricClient,
) {
  companion object {
    fun newAccessTokenInterceptorFromConfig(config: InternalApiClientConfig) =
      AccessTokenInterceptor(
        clientId = config.auth.clientId!!,
        clientSecret = config.auth.clientSecret!!,
        tokenEndpoint = config.auth.tokenEndpoint!!,
        httpClient =
          OkHttpClient
            .Builder()
            .readTimeout(config.readTimeoutSeconds, TimeUnit.SECONDS)
            .connectTimeout(config.connectTimeoutSeconds, TimeUnit.SECONDS)
            .build(),
      )
  }

  @Singleton
  @Requires(property = "airbyte.internal-api.base-path")
  fun airbyteInternalApiClient(): AirbyteApiClient {
    val httpClient =
      OkHttpClient
        .Builder()
        .apply {
          if (config.throwsOn5xx) {
            addInterceptor(ThrowOn5xxInterceptor())
          }

          when (config.auth.type) {
            InternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN -> {
              addInterceptor(newAccessTokenInterceptorFromConfig(config))
            }
            InternalApiClientConfig.AuthType.KEYCLOAK_ACCESS_TOKEN -> {
              addInterceptor(keycloakAccessTokenInterceptor!!)
            }
            InternalApiClientConfig.AuthType.STATIC_TOKEN -> {
              if (config.auth.token.isNullOrBlank()) {
                throw Exception("setting up static auth interceptor: token is null or blank")
              }
              addInterceptor(StaticTokenInterceptor(config.auth.token))
            }
          }

          addInterceptor(UserAgentInterceptor(applicationName))
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

    return AirbyteApiClient(config.basePath!!, retryPolicy, httpClient)
  }
}
