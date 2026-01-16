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
internal class AirbyteDataPlaneQueueConfigDefaultTest {
  @Inject
  private lateinit var airbyteDataPlaneQueueConfig: AirbyteDataPlaneQueueConfig

  @Test
  fun testLoadingDefaultValues() {
    assertEquals(DEFAULT_DATA_PLANE_QUEUE_CHECK, airbyteDataPlaneQueueConfig.check.taskQueue)
    assertEquals(DEFAULT_DATA_PLANE_QUEUE_DISCOVER, airbyteDataPlaneQueueConfig.discover.taskQueue)
    assertEquals(DEFAULT_DATA_PLANE_QUEUE_SYNC, airbyteDataPlaneQueueConfig.sync.taskQueue)
  }
}

@MicronautTest(propertySources = ["classpath:application-data-plane-queue.yml"])
internal class AirbyteDataPlaneQueueConfigOverridesTest {
  @Inject
  private lateinit var airbyteDataPlaneQueueConfig: AirbyteDataPlaneQueueConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("CHECK_CONNECTION_OVERRIDE", airbyteDataPlaneQueueConfig.check.taskQueue)
    assertEquals("DISCOVER_SCHEMA_OVERRIDE", airbyteDataPlaneQueueConfig.discover.taskQueue)
    assertEquals("SYNC_OVERRIDE", airbyteDataPlaneQueueConfig.sync.taskQueue)
  }
}
