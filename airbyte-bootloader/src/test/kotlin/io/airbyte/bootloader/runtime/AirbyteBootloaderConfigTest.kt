/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteBootloaderConfigDefaultTest {
  @Inject
  private lateinit var airbyteBootloaderConfig: AirbyteBootloaderConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteBootloaderConfig.autoUpgradeConnectors)
    assertEquals(DEFAULT_MIGRATION_BASELINE_VERSION, airbyteBootloaderConfig.migrationBaselineVersion)
    assertEquals(true, airbyteBootloaderConfig.runMigrationAtStartup)
  }
}

@MicronautTest(propertySources = ["classpath:application-bootloader.yml"], environments = [Environment.TEST])
internal class AirbyteBootloaderConfigOverridesTest {
  @Inject
  private lateinit var airbyteBootloaderConfig: AirbyteBootloaderConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteBootloaderConfig.autoUpgradeConnectors)
    assertEquals("1.0.0", airbyteBootloaderConfig.migrationBaselineVersion)
    assertEquals(false, airbyteBootloaderConfig.runMigrationAtStartup)
  }
}
