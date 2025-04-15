/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck
import io.airbyte.db.check.ConfigsDatabaseMigrationCheck
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.check.JobsDatabaseMigrationCheck
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createConfigsDatabaseAvailabilityCheck
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createConfigsDatabaseInitializer
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createConfigsDatabaseMigrationCheck
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createJobsDatabaseAvailabilityCheck
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createJobsDatabaseInitializer
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createJobsDatabaseMigrationCheck
import io.airbyte.db.init.ConfigsDatabaseInitializer
import io.airbyte.db.init.JobsDatabaseInitializer
import io.mockk.mockk
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import kotlin.test.assertIs

/**
 * Test suite for the [DatabaseCheckFactory] class.
 */
internal class DatabaseCheckFactoryTest {
  @Test
  fun testCreateConfigsDatabaseAvailabilityCheck() {
    val dslContext = mockk<DSLContext>()
    val timeoutMs = 500L
    val check = createConfigsDatabaseAvailabilityCheck(dslContext, timeoutMs)

    assertNotNull(check)
    assertIs<ConfigsDatabaseAvailabilityCheck>(check)
    assertEquals(timeoutMs, check.timeoutMs)
    assertNotNull(check.dslContext)
    assertEquals(dslContext, check.dslContext)
  }

  @Test
  fun testCreateJobsDatabaseAvailabilityCheck() {
    val dslContext = mockk<DSLContext>()
    val timeoutMs = 500L
    val check = createJobsDatabaseAvailabilityCheck(dslContext, timeoutMs)

    assertNotNull(check)
    assertIs<JobsDatabaseAvailabilityCheck>(check)
    assertEquals(timeoutMs, check.timeoutMs)
    assertNotNull(check.dslContext)
    assertEquals(dslContext, check.dslContext)
  }

  @Test
  fun testCreateConfigsDatabaseMigrationCheck() {
    val dslContext = mockk<DSLContext>()
    val flyway = mockk<Flyway>()
    val minimumMigrationVersion = "1.2.3"
    val timeoutMs = 500L
    val check = createConfigsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs)

    assertNotNull(check)
    assertIs<ConfigsDatabaseMigrationCheck>(check)
    assertNotNull(check.databaseAvailabilityCheck)
    assertIs<ConfigsDatabaseAvailabilityCheck>(check.databaseAvailabilityCheck)
    assertEquals(minimumMigrationVersion, check.minimumFlywayVersion)
    assertEquals(timeoutMs, check.timeoutMs)
    assertNotNull(check.flyway)
    assertEquals(flyway, check.flyway)
  }

  @Test
  fun testCreateJobsDatabaseMigrationCheck() {
    val dslContext = mockk<DSLContext>()
    val flyway = mockk<Flyway>()
    val minimumMigrationVersion = "1.2.3"
    val timeoutMs = 500L
    val check = createJobsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs)

    assertNotNull(check)
    assertIs<JobsDatabaseMigrationCheck>(check)
    assertNotNull(check.databaseAvailabilityCheck)
    assertIs<JobsDatabaseAvailabilityCheck>(check.databaseAvailabilityCheck)
    assertEquals(minimumMigrationVersion, check.minimumFlywayVersion)
    assertEquals(timeoutMs, check.timeoutMs)
    assertNotNull(check.flyway)
    assertEquals(flyway, check.flyway)
  }

  @Test
  fun testCreateConfigsDatabaseInitializer() {
    val dslContext = mockk<DSLContext>()
    val initialSchema = "SELECT 1;"
    val timeoutMs = 500L
    val initializer = createConfigsDatabaseInitializer(dslContext, timeoutMs, initialSchema)

    assertNotNull(initializer)
    assertIs<ConfigsDatabaseInitializer>(initializer)
    assertNotNull(initializer.databaseAvailabilityCheck)
    assertIs<ConfigsDatabaseAvailabilityCheck>(initializer.databaseAvailabilityCheck)
    assertEquals(timeoutMs, initializer.databaseAvailabilityCheck!!.timeoutMs)
    assertNotNull(initializer.dslContext)
    assertEquals(dslContext, initializer.dslContext)
    assertEquals(initialSchema, initializer.initialSchema)
  }

  @Test
  fun testCreateJobsDatabaseInitializer() {
    val dslContext = mockk<DSLContext>()
    val initialSchema = "SELECT 1;"
    val timeoutMs = 500L
    val initializer = createJobsDatabaseInitializer(dslContext, timeoutMs, initialSchema)

    assertNotNull(initializer)
    assertIs<JobsDatabaseInitializer>(initializer)
    assertNotNull(initializer.databaseAvailabilityCheck)
    assertIs<JobsDatabaseAvailabilityCheck>(initializer.databaseAvailabilityCheck)
    assertEquals(timeoutMs, initializer.databaseAvailabilityCheck!!.timeoutMs)
    assertNotNull(initializer.dslContext)
    assertEquals(dslContext, initializer.dslContext)
    assertEquals(initialSchema, initializer.initialSchema)
  }
}
