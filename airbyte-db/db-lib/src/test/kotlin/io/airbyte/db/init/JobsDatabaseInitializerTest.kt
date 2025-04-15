/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.commons.resources.Resources
import io.airbyte.db.check.DatabaseCheckException
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.instance.DatabaseConstants
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

/**
 * Test suite for the [JobsDatabaseInitializer] class.
 */
internal class JobsDatabaseInitializerTest : CommonDatabaseInitializerTest() {
  @Test
  fun testInitializingSchema() {
    val databaseAvailabilityCheck = mockk<JobsDatabaseAvailabilityCheck>(relaxed = true)
    val initialSchema = Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH)
    val initializer = JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)

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
    val databaseAvailabilityCheck = mockk<JobsDatabaseAvailabilityCheck>(relaxed = true)
    val initialSchema = Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH)
    dslContext.execute(initialSchema)
    val initializer = JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)

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
      mockk<JobsDatabaseAvailabilityCheck> {
        every { check() } throws DatabaseCheckException("test")
      }
    val initialSchema = Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH)

    val initializer = JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema)
    assertThrows<DatabaseInitializationException> { initializer.initialize() }
  }
}
