/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.manifestserver.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Factory
class ManifestServerApiClientFactory {
  @Singleton
  @Requires(property = "airbyte.manifest-server-api.base-path")
  fun manifestServerApiClient(
    @Property(name = "micronaut.application.name") applicationName: String,
    @Property(name = "airbyte.manifest-server-api.base-path") basePath: String,
    @Property(name = "airbyte.manifest-server-api.connect-timeout-seconds") connectTimeoutSeconds: Long,
    @Property(name = "airbyte.manifest-server-api.read-timeout-seconds") readTimeoutSeconds: Long,
    @Property(name = "airbyte.manifest-server-api.signature-secret") signatureSecret: String,
  ): ManifestServerApiClient {
    val builder: OkHttpClient.Builder =
      OkHttpClient.Builder().apply {
        addInterceptor(InternalClientTokenInterceptor(applicationName, signatureSecret))
        readTimeout(readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(connectTimeoutSeconds.seconds.toJavaDuration())
      }

    val okHttpClient: OkHttpClient = builder.build()
    val retryPolicy: RetryPolicy<Response> = RetryPolicy.builder<Response>().withMaxRetries(0).build()

    return ManifestServerApiClient(
      basePath = basePath,
      policy = retryPolicy,
      httpClient = okHttpClient,
    )
  }
}
