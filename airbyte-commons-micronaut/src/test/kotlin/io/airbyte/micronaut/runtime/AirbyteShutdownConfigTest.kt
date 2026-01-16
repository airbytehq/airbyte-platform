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
internal class AirbyteShutdownConfigDefaultTest {
  @Inject
  private lateinit var airbyteShutdownConfig: AirbyteShutdownConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_SHUTDOWN_DELAY_MS, airbyteShutdownConfig.delayMs)
  }
}

@MicronautTest(propertySources = ["classpath:application-shutdown.yml"])
internal class AirbyteShutdownConfigOverridesTest {
  @Inject
  private lateinit var airbyteShutdownConfig: AirbyteShutdownConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(100L, airbyteShutdownConfig.delayMs)
  }
}
