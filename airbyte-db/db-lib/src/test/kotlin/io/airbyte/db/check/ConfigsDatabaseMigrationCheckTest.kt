/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.mockk.every
import io.mockk.mockk
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationInfo
import org.flywaydb.core.api.MigrationInfoService
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

private const val CURRENT_VERSION = "1.2.3"
private const val VERSION_2 = "2.0.0"

/**
 * Test suite for the [ConfigsDatabaseMigrationCheck] class.
 */
internal class ConfigsDatabaseMigrationCheckTest {
  @Test
  fun testMigrationCheck() {
    val minimumVersion = "1.0.0"
    val migrationVersion = MigrationVersion.fromVersion(CURRENT_VERSION)

    val migrationInfo =
      mockk<MigrationInfo> {
        every { version } returns migrationVersion
      }
    val migrationInfoService =
      mockk<MigrationInfoService> {
        every { current() } returns migrationInfo
      }
    val flyway =
      mockk<Flyway> {
        every { info() } returns migrationInfoService
      }
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)
    val check = ConfigsDatabaseMigrationCheck(databaseAvailabilityCheck, flyway, minimumVersion, CommonsDatabaseCheckTest.TIMEOUT_MS)
    assertDoesNotThrow { check.check() }
  }

  @Test
  fun testMigrationCheckEqualVersion() {
    val minimumVersion = "1.2.3"
    val migrationVersion = MigrationVersion.fromVersion(minimumVersion)
    val migrationInfo =
      mockk<MigrationInfo> {
        every { version } returns migrationVersion
      }
    val migrationInfoService =
      mockk<MigrationInfoService> {
        every { current() } returns migrationInfo
      }
    val flyway =
      mockk<Flyway> {
        every { info() } returns migrationInfoService
      }
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)

    val check = ConfigsDatabaseMigrationCheck(databaseAvailabilityCheck, flyway, minimumVersion, CommonsDatabaseCheckTest.TIMEOUT_MS)
    assertDoesNotThrow { check.check() }
  }

  @Test
  fun testMigrationCheckTimeout() {
    val migrationVersion = MigrationVersion.fromVersion(CURRENT_VERSION)
    val migrationInfo =
      mockk<MigrationInfo> {
        every { version } returns migrationVersion
      }
    val migrationInfoService =
      mockk<MigrationInfoService> {
        every { current() } returns migrationInfo
      }
    val flyway =
      mockk<Flyway> {
        every { info() } returns migrationInfoService
      }
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)

    val check = ConfigsDatabaseMigrationCheck(databaseAvailabilityCheck, flyway, VERSION_2, CommonsDatabaseCheckTest.TIMEOUT_MS)
    assertThrows<DatabaseCheckException> { check.check() }
  }

  @Test
  fun unavailableFlywayMigrationVersion() {
    val migrationInfoService =
      mockk<MigrationInfoService> {
        every { current() } returns null
      }
    val flyway =
      mockk<Flyway> {
        every { info() } returns migrationInfoService
      }
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)

    val check = ConfigsDatabaseMigrationCheck(databaseAvailabilityCheck, flyway, VERSION_2, CommonsDatabaseCheckTest.TIMEOUT_MS)
    assertThrows<DatabaseCheckException> { check.check() }
  }
}
