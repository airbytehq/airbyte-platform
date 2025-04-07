/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.commons.resources.Resources
import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck
import io.airbyte.db.check.DatabaseCheckException
import io.airbyte.db.instance.DatabaseConstants
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

/**
 * Test suite for the [ConfigsDatabaseInitializer] class.
 */
internal class ConfigsDatabaseInitializerTest : CommonDatabaseInitializerTest() {
  @Test
  fun testInitializingSchema() {
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)
    val initialSchema = Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH)
    val initializer = ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)

    assertDoesNotThrow { initializer.initialize() }
    assertTrue(
      initializer.hasTable(
        dslContext,
        initializer.tableNames.first(),
      ),
    )
  }

  @Test
  fun testInitializingSchemaAlreadyExists() {
    val databaseAvailabilityCheck = mockk<ConfigsDatabaseAvailabilityCheck>(relaxed = true)
    val initialSchema = Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH)
    dslContext.execute(initialSchema)
    val initializer = ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)

    assertDoesNotThrow { initializer.initialize() }
    assertTrue(
      initializer.hasTable(
        dslContext,
        initializer.tableNames.first(),
      ),
    )
  }

  @Test
  fun testInitializationException() {
    val databaseAvailabilityCheck =
      mockk<ConfigsDatabaseAvailabilityCheck> {
        every { check() } throws DatabaseCheckException("test")
      }

    val initialSchema = Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH)

    val initializer = ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)
    assertThrows<DatabaseInitializationException> { initializer.initialize() }
  }
}
