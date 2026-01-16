/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteWorkerActivityConfigDefaultTest {
  @Inject
  private lateinit var airbyteWorkerActivityConfig: AirbyteWorkerActivityConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_WORKER_ACTIVITY_INITIAL_DELAY_SECONDS, airbyteWorkerActivityConfig.initialDelay)
    assertEquals(DEFAULT_WORKER_ACTIVITY_MAX_ATTEMPTS, airbyteWorkerActivityConfig.maxAttempts)
    assertEquals(DEFAULT_WORKER_ACTIVITY_MAX_DELAY_SECONDS, airbyteWorkerActivityConfig.maxDelay)
    assertEquals(DEFAULT_WORKER_ACTIVITY_MAX_TIMEOUT_SECONDS, airbyteWorkerActivityConfig.maxTimeout)
    assertEquals(DEFAULT_WORKER_ACTIVITY_CHECK_TIMEOUT_SECONDS, airbyteWorkerActivityConfig.checkTimeout)
    assertEquals(DEFAULT_WORKER_ACTIVITY_DISCOVERY_TIMEOUT_SECONDS, airbyteWorkerActivityConfig.discoveryTimeout)
  }
}

@MicronautTest(propertySources = ["classpath:application-worker-activity.yml"])
internal class AirbyteWorkerActivityConfigOverridesTest {
  @Inject
  private lateinit var airbyteWorkerActivityConfig: AirbyteWorkerActivityConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(1, airbyteWorkerActivityConfig.initialDelay)
    assertEquals(2, airbyteWorkerActivityConfig.maxAttempts)
    assertEquals(3, airbyteWorkerActivityConfig.maxDelay)
    assertEquals(4L, airbyteWorkerActivityConfig.maxTimeout)
    assertEquals(5L, airbyteWorkerActivityConfig.checkTimeout)
    assertEquals(6L, airbyteWorkerActivityConfig.discoveryTimeout)
  }
}
