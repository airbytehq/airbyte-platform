/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory
import io.micronaut.context.annotation.Value
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
  @Value("\${airbyte.kubernetes.client.call-timeout-sec}") private val callTimeout: Long,
  @Value("\${airbyte.kubernetes.client.connect-timeout-sec}") private val connectTimeout: Long,
  @Value("\${airbyte.kubernetes.client.connection-pool.keep-alive-sec}") private val keepAliveDuration: Long,
  @Value("\${airbyte.kubernetes.client.connection-pool.max-idle-connections}") private val maxIdleConnections: Int,
  @Value("\${airbyte.kubernetes.client.read-timeout-sec}") private val readTimeout: Long,
  @Value("\${airbyte.kubernetes.client.write-timeout-sec}") private val writeTimeout: Long,
) : OkHttpClientFactory() {
  override fun additionalConfig(builder: OkHttpClient.Builder?) {
    builder?.apply {
      callTimeout(callTimeout, TimeUnit.SECONDS)
      connectionPool(ConnectionPool(maxIdleConnections, keepAliveDuration, TimeUnit.SECONDS))
      connectTimeout(connectTimeout, TimeUnit.SECONDS)
      readTimeout(readTimeout, TimeUnit.SECONDS)
      // Retry on Connectivity issues (Unreachable IP/Proxy, Stale Pool Connection)
      retryOnConnectionFailure(true)
      writeTimeout(writeTimeout, TimeUnit.SECONDS)
    }
  }
}
