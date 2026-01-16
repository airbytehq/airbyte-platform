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
internal class AirbyteFlywayConfigDefaultTest {
  @Inject
  private lateinit var airbyteFlywayConfig: AirbyteFlywayConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_FLYWAY_INITIALIZATION_TIMEOUT_MS, airbyteFlywayConfig.config.initializationTimeoutMs)
    assertEquals("", airbyteFlywayConfig.config.minimumMigrationVersion)
    assertEquals(DEFAULT_FLYWAY_INITIALIZATION_TIMEOUT_MS, airbyteFlywayConfig.jobs.initializationTimeoutMs)
    assertEquals("", airbyteFlywayConfig.jobs.minimumMigrationVersion)
  }
}

@MicronautTest(propertySources = ["classpath:application-flyway.yml"])
internal class AirbyteFlywayConfigOverridesTest {
  @Inject
  private lateinit var airbyteFlywayConfig: AirbyteFlywayConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(10L, airbyteFlywayConfig.config.initializationTimeoutMs)
    assertEquals("1.0.0", airbyteFlywayConfig.config.minimumMigrationVersion)
    assertEquals(20L, airbyteFlywayConfig.jobs.initializationTimeoutMs)
    assertEquals("2.0.0", airbyteFlywayConfig.jobs.minimumMigrationVersion)
  }
}
