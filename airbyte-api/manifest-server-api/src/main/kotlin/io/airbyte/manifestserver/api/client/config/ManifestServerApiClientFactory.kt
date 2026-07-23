/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.manifestserver.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.InternalClientTokenInterceptor
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.micronaut.runtime.AirbyteManifestServerApiClientConfig
import io.airbyte.micronaut.runtime.MANIFEST_SERVER_API_PREFIX
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Factory
class ManifestServerApiClientFactory {
  @Singleton
  @Requires(property = "${MANIFEST_SERVER_API_PREFIX}.base-path", pattern = ".+")
  fun manifestServerApiClient(
    airbyteManifestServerApiClientConfig: AirbyteManifestServerApiClientConfig,
    micronautApplicationConfiguration: ApplicationConfiguration,
  ): ManifestServerApiClient {
    val builder: OkHttpClient.Builder =
      OkHttpClient.Builder().apply {
        addInterceptor(
          InternalClientTokenInterceptor(
            micronautApplicationConfiguration.name.get(),
            airbyteManifestServerApiClientConfig.signatureSecret,
          ),
        )
        readTimeout(airbyteManifestServerApiClientConfig.readTimeoutSeconds.seconds.toJavaDuration())
        connectTimeout(airbyteManifestServerApiClientConfig.connectTimeoutSeconds.seconds.toJavaDuration())
      }

    val okHttpClient: OkHttpClient = builder.build()
    val retryPolicy: RetryPolicy<Response> = RetryPolicy.builder<Response>().withMaxRetries(0).build()

    return ManifestServerApiClient(
      basePath = airbyteManifestServerApiClientConfig.basePath,
      policy = retryPolicy,
      httpClient = okHttpClient,
    )
  }
}
