/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.development

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.db.instance.development.AIRBYTE_VERSION_ENV_VAR
import io.airbyte.db.instance.development.MigrationDevHelper.currentAirbyteVersion
import io.airbyte.db.instance.development.MigrationDevHelper.getNextMigrationVersion
import io.airbyte.db.instance.development.VERSION_ENV_VAR
import io.airbyte.db.instance.development.airbyteVersion
import io.airbyte.db.instance.development.format
import io.airbyte.db.instance.development.migrationId
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junitpioneer.jupiter.ClearEnvironmentVariable
import org.junitpioneer.jupiter.SetEnvironmentVariable

private const val VERSION_0113_ALPHA = "0.11.3-alpha"

internal class MigrationDevHelperTest {
  @SetEnvironmentVariable(key = AIRBYTE_VERSION_ENV_VAR, value = VERSION_0113_ALPHA)
  @Test
  fun testGetCurrentAirbyteVersion() {
    // Test that this method will not throw any exception.
    Assertions.assertEquals(VERSION_0113_ALPHA, currentAirbyteVersion.serialize())
  }

  @SetEnvironmentVariable(key = VERSION_ENV_VAR, value = "dev")
  @Test
  fun testGetCurrentAirbyteVersionFromFallbackEnv() {
    // Test that this method will not throw any exception.
    Assertions.assertEquals("dev", currentAirbyteVersion.serialize())
  }

  @ClearEnvironmentVariable(key = AIRBYTE_VERSION_ENV_VAR)
  @ClearEnvironmentVariable(key = VERSION_ENV_VAR)
  @Test
  fun testGetCurrentAirbyteVersionFailure() {
    Assertions.assertThrows(
      IllegalStateException::class.java,
    ) { currentAirbyteVersion.toString() }
  }

  @Test
  fun testGetAirbyteVersion() {
    val migrationVersion = MigrationVersion.fromVersion("0.11.3.010")
    val airbyteVersion = migrationVersion.airbyteVersion
    Assertions.assertEquals("0.11.3", airbyteVersion.serialize())
  }

  @Test
  fun testFormatAirbyteVersion() {
    val airbyteVersion = AirbyteVersion(VERSION_0113_ALPHA)
    Assertions.assertEquals("0_11_3", airbyteVersion.format())
  }

  @Test
  fun testGetMigrationId() {
    val migrationVersion = MigrationVersion.fromVersion("0.11.3.010")
    Assertions.assertEquals("010", migrationVersion.migrationId)
  }

  @Test
  fun testGetNextMigrationVersion() {
    // Migration version does not exist
    Assertions.assertEquals(
      "0.11.3.001",
      getNextMigrationVersion(
        AirbyteVersion(VERSION_0113_ALPHA),
        null,
      ).version,
    )

    // Airbyte version is greater
    Assertions.assertEquals(
      "0.11.3.001",
      getNextMigrationVersion(
        AirbyteVersion(VERSION_0113_ALPHA),
        MigrationVersion.fromVersion("0.10.9.003"),
      ).version,
    )

    // Airbyte version is equal to migration version
    Assertions.assertEquals(
      "0.11.3.004",
      getNextMigrationVersion(
        AirbyteVersion(VERSION_0113_ALPHA),
        MigrationVersion.fromVersion("0.11.3.003"),
      ).version,
    )

    // Migration version is greater
    Assertions.assertEquals(
      "0.11.3.004",
      getNextMigrationVersion(
        AirbyteVersion("0.9.17-alpha"),
        MigrationVersion.fromVersion("0.11.3.003"),
      ).version,
    )
  }
}
