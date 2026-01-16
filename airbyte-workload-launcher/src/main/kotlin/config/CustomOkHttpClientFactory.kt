/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.micronaut.runtime.AirbyteKubernetesConfig
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory
import jakarta.inject.Singleton
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

/**
 * Custom {@link OkHttpClientFactory} implementation that allows for configuration
 * of the underlying OkHttp client.
 */
@Singleton
class CustomOkHttpClientFactory(
  private val airbyteKubernetesConfig: AirbyteKubernetesConfig,
) : OkHttpClientFactory() {
  override fun additionalConfig(builder: OkHttpClient.Builder?) {
    builder?.apply {
      callTimeout(airbyteKubernetesConfig.client.callTimeoutSec, TimeUnit.SECONDS)
      connectionPool(
        ConnectionPool(
          airbyteKubernetesConfig.client.connectionPool.maxIdleConnections,
          airbyteKubernetesConfig.client.connectionPool.keepAliveSec,
          TimeUnit.SECONDS,
        ),
      )
      connectTimeout(airbyteKubernetesConfig.client.connectTimeoutSec, TimeUnit.SECONDS)
      readTimeout(airbyteKubernetesConfig.client.readTimeoutSec, TimeUnit.SECONDS)
      // Retry on Connectivity issues (Unreachable IP/Proxy, Stale Pool Connection)
      retryOnConnectionFailure(true)
      writeTimeout(airbyteKubernetesConfig.client.writeTimeoutSec, TimeUnit.SECONDS)
    }
  }
}
