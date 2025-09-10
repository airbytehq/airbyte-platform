/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.connectorbuilderserver.api.client.ConnectorBuilderServerApiClient
import io.airbyte.micronaut.runtime.AirbyteConnectorBuilderApiConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Factory
class ConnectorBuilderServerApiClientFactory {
  @Singleton
  @Requires(property = "airbyte.connector-builder-server-api.base-path", pattern = ".+")
  fun connectorBuilderServerApiClient(
    applicationConfiguration: ApplicationConfiguration,
    airbyteConnectorBuilderApiConfig: AirbyteConnectorBuilderApiConfig,
  ): ConnectorBuilderServerApiClient {
    val builder: OkHttpClient.Builder =
      OkHttpClient.Builder().apply {
        addInterceptor(InternalClientTokenInterceptor(applicationConfiguration.name.get(), airbyteConnectorBuilderApiConfig.signatureSecret))
        readTimeout(airbyteConnectorBuilderApiConfig.readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(airbyteConnectorBuilderApiConfig.connectTimeoutSeconds.seconds.toJavaDuration())
      }

    val okHttpClient: OkHttpClient = builder.build()
    val retryPolicy: RetryPolicy<Response> = RetryPolicy.builder<Response>().withMaxRetries(0).build()

    return ConnectorBuilderServerApiClient(
      basePath = airbyteConnectorBuilderApiConfig.basePath,
      policy = retryPolicy,
      httpClient = okHttpClient,
    )
  }
}
