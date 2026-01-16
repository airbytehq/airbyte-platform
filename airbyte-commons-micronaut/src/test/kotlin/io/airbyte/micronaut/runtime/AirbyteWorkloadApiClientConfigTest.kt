/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteWorkloadApiClientConfigDefaultTest {
  @Inject
  private lateinit var airbyteWorkloadApiClientConfig: AirbyteWorkloadApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteWorkloadApiClientConfig.basePath)
    assertEquals(30, airbyteWorkloadApiClientConfig.connectTimeoutSeconds)
    assertEquals(600, airbyteWorkloadApiClientConfig.readTimeoutSeconds)
    assertEquals(2, airbyteWorkloadApiClientConfig.retries.delaySeconds)
    assertEquals(.25, airbyteWorkloadApiClientConfig.retries.jitterFactor)
    assertEquals(5, airbyteWorkloadApiClientConfig.retries.max)
    assertEquals(10, airbyteWorkloadApiClientConfig.heartbeat.intervalSeconds)
    assertEquals(600, airbyteWorkloadApiClientConfig.heartbeat.timeoutSeconds)
  }
}

@MicronautTest(propertySources = ["classpath:application-workload-api.yml"])
internal class AirbyteWorkloadApiClientConfigTest {
  @Inject
  private lateinit var airbyteWorkloadApiClientConfig: AirbyteWorkloadApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://localhost:8002", airbyteWorkloadApiClientConfig.basePath)
    assertEquals(90, airbyteWorkloadApiClientConfig.connectTimeoutSeconds)
    assertEquals(150, airbyteWorkloadApiClientConfig.readTimeoutSeconds)
    assertEquals(21, airbyteWorkloadApiClientConfig.retries.delaySeconds)
    assertEquals(1.125, airbyteWorkloadApiClientConfig.retries.jitterFactor)
    assertEquals(15, airbyteWorkloadApiClientConfig.retries.max)
    assertEquals(100, airbyteWorkloadApiClientConfig.heartbeat.intervalSeconds)
    assertEquals(10, airbyteWorkloadApiClientConfig.heartbeat.timeoutSeconds)
  }
}
