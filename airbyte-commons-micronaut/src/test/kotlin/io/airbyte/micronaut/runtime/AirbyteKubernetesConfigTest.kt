/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteKubernetesConfigDefaultTest {
  @Inject
  private lateinit var airbyteKubernetesConfig: AirbyteKubernetesConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_KUBERNETES_CLIENT_CALL_TIMEOUT_SECONDS, airbyteKubernetesConfig.client.callTimeoutSec)
    assertEquals(DEFAULT_KUBERNETES_CLIENT_CONNECT_TIMEOUT_SECONDS, airbyteKubernetesConfig.client.connectTimeoutSec)
    assertEquals(DEFAULT_KUBERNETES_CLIENT_READ_TIMEOUT_SECONDS, airbyteKubernetesConfig.client.readTimeoutSec)
    assertEquals(DEFAULT_KUBERNETES_CLIENT_WRITE_TIMEOUT_SECONDS, airbyteKubernetesConfig.client.writeTimeoutSec)
    assertEquals(DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_KEEP_ALIVE_SECONDS, airbyteKubernetesConfig.client.connectionPool.keepAliveSec)
    assertEquals(
      DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_MAX_IDLE_CONNECTIONS,
      airbyteKubernetesConfig.client.connectionPool.maxIdleConnections,
    )
    assertEquals(DEFAULT_KUBERNETES_CLIENT_RETRY_DELAY_SECONDS, airbyteKubernetesConfig.client.retries.delaySeconds)
    assertEquals(DEFAULT_KUBERNETES_CLIENT_RETRY_MAX, airbyteKubernetesConfig.client.retries.max)
    assertEquals(Duration.parse(DEFAULT_KUBERNETES_RESOURCE_CHECK_RATE_SECONDS), airbyteKubernetesConfig.resourceCheckRate)
  }
}

@MicronautTest(propertySources = ["classpath:application-kubernetes.yml"])
internal class AirbyteKubernetesConfigOverridesTest {
  @Inject
  private lateinit var airbyteKubernetesConfig: AirbyteKubernetesConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(1, airbyteKubernetesConfig.client.callTimeoutSec)
    assertEquals(2, airbyteKubernetesConfig.client.connectTimeoutSec)
    assertEquals(3, airbyteKubernetesConfig.client.readTimeoutSec)
    assertEquals(4, airbyteKubernetesConfig.client.writeTimeoutSec)
    assertEquals(5, airbyteKubernetesConfig.client.connectionPool.keepAliveSec)
    assertEquals(6, airbyteKubernetesConfig.client.connectionPool.maxIdleConnections)
    assertEquals(7, airbyteKubernetesConfig.client.retries.delaySeconds)
    assertEquals(8, airbyteKubernetesConfig.client.retries.max)
    assertEquals(Duration.parse("PT10S"), airbyteKubernetesConfig.resourceCheckRate)
  }
}
