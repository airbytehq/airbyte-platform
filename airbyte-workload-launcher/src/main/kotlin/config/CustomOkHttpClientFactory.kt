/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
  @Value("\${airbyte.kubernetes.client.call-timeout-sec}") private val callTimeout: java.lang.Long,
  @Value("\${airbyte.kubernetes.client.connect-timeout-sec}") private val connectTimeout: java.lang.Long,
  @Value("\${airbyte.kubernetes.client.connection-pool.keep-alive-sec}") private val keepAliveDuration: java.lang.Long,
  @Value("\${airbyte.kubernetes.client.connection-pool.max-idle-connections}") private val maxIdleConnections: Integer,
  @Value("\${airbyte.kubernetes.client.read-timeout-sec}") private val readTimeout: java.lang.Long,
  @Value("\${airbyte.kubernetes.client.write-timeout-sec}") private val writeTimeout: java.lang.Long,
) : OkHttpClientFactory() {
  override fun additionalConfig(builder: OkHttpClient.Builder?) {
    builder?.apply {
      callTimeout(callTimeout.toLong(), TimeUnit.SECONDS)
      connectionPool(ConnectionPool(maxIdleConnections.toInt(), keepAliveDuration.toLong(), TimeUnit.SECONDS))
      connectTimeout(connectTimeout.toLong(), TimeUnit.SECONDS)
      readTimeout(readTimeout.toLong(), TimeUnit.SECONDS)
      retryOnConnectionFailure(false)
      writeTimeout(writeTimeout.toLong(), TimeUnit.SECONDS)
    }
  }
}
