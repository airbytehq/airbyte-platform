/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Factory
class ConnectorBuilderServerApiClientFactory {
  @Singleton
  @Requires(property = "airbyte.connector-builder-server-api.base-path")
  fun connectorBuilderServerApiClient(
    @Property(name = "micronaut.application.name") applicationName: String,
    @Property(name = "airbyte.connector-builder-server-api.base-path") basePath: String,
    @Property(name = "airbyte.connector-builder-server-api.signature-secret") signatureSecret: String,
    @Property(name = "airbyte.connector-builder-server-api.connect-timeout-seconds") connectTimeoutSeconds: Long,
    @Property(name = "airbyte.connector-builder-server-api.read-timeout-seconds") readTimeoutSeconds: Long,
  ): ConnectorBuilderServerApi {
    val builder: OkHttpClient.Builder =
      OkHttpClient.Builder().apply {
        addInterceptor(InternalClientTokenInterceptor(applicationName, signatureSecret))
        readTimeout(readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(connectTimeoutSeconds.seconds.toJavaDuration())
      }

    val okHttpClient: OkHttpClient = builder.build()
    val retryPolicy: RetryPolicy<Response> = RetryPolicy.builder<Response>().withMaxRetries(0).build()

    return ConnectorBuilderServerApi(
      basePath = basePath,
      policy = retryPolicy,
      client = okHttpClient,
    )
  }
}
