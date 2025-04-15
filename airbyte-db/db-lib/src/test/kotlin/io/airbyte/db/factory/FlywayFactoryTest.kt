/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.test.utils.Databases
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val INSTALLED_BY = "test"
private const val DB_IDENTIFIER = "test"

/**
 * Test suite for the [FlywayFactory] class.
 */
internal class FlywayFactoryTest : CommonFactoryTest() {
  @Test
  fun testCreatingAFlywayInstance() {
    val baselineVersion = "1.2.3"
    val baselineDescription = "A test baseline description"
    val baselineOnMigrate = true
    val migrationFileLocation = "classpath:io/airbyte/db/instance/toys/migrations"
    val dataSource = Databases.createDataSource(container)
    val flyway = create(dataSource, INSTALLED_BY, DB_IDENTIFIER, baselineVersion, baselineDescription, baselineOnMigrate, migrationFileLocation)

    assertNotNull(flyway)
    assertTrue(flyway.configuration.isBaselineOnMigrate)
    assertEquals(baselineDescription, flyway.configuration.baselineDescription)
    assertEquals(baselineVersion, flyway.configuration.baselineVersion.version)
    assertEquals(baselineOnMigrate, flyway.configuration.isBaselineOnMigrate)
    assertEquals(INSTALLED_BY, flyway.configuration.installedBy)
    assertEquals(String.format(FlywayFactory.MIGRATION_TABLE_FORMAT, DB_IDENTIFIER), flyway.configuration.table)
    assertEquals(migrationFileLocation, flyway.configuration.locations[0].descriptor)
  }

  @Test
  fun testCreatingAFlywayInstanceWithDefaults() {
    val migrationFileLocation = "classpath:io/airbyte/db/instance/toys/migrations"
    val dataSource = Databases.createDataSource(container)
    val flyway = create(dataSource, INSTALLED_BY, DB_IDENTIFIER, migrationFileLocation)

    assertNotNull(flyway)
    assertTrue(flyway.configuration.isBaselineOnMigrate)
    assertEquals(FlywayFactory.BASELINE_DESCRIPTION, flyway.configuration.baselineDescription)
    assertEquals(FlywayFactory.BASELINE_VERSION, flyway.configuration.baselineVersion.version)
    assertEquals(FlywayFactory.BASELINE_ON_MIGRATION, flyway.configuration.isBaselineOnMigrate)
    assertEquals(INSTALLED_BY, flyway.configuration.installedBy)
    assertEquals(String.format(FlywayFactory.MIGRATION_TABLE_FORMAT, DB_IDENTIFIER), flyway.configuration.table)
    assertEquals(migrationFileLocation, flyway.configuration.locations[0].descriptor)
  }
}
