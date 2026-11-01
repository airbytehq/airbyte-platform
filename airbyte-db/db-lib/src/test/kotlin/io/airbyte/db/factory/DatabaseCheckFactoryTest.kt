/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

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
    val configsDatabaseAvailabilityCheck = assertInstanceOf(ConfigsDatabaseAvailabilityCheck::class.java, check)
    assertEquals(timeoutMs, configsDatabaseAvailabilityCheck.timeoutMs)
    assertNotNull(configsDatabaseAvailabilityCheck.dslContext)
    assertEquals(dslContext, configsDatabaseAvailabilityCheck.dslContext)
  }

  @Test
  fun testCreateJobsDatabaseAvailabilityCheck() {
    val dslContext = mockk<DSLContext>()
    val timeoutMs = 500L
    val check = createJobsDatabaseAvailabilityCheck(dslContext, timeoutMs)

    assertNotNull(check)
    val jobsDatabaseAvailabilityCheck = assertInstanceOf(JobsDatabaseAvailabilityCheck::class.java, check)
    assertEquals(timeoutMs, jobsDatabaseAvailabilityCheck.timeoutMs)
    assertNotNull(jobsDatabaseAvailabilityCheck.dslContext)
    assertEquals(dslContext, jobsDatabaseAvailabilityCheck.dslContext)
  }

  @Test
  fun testCreateConfigsDatabaseMigrationCheck() {
    val dslContext = mockk<DSLContext>()
    val flyway = mockk<Flyway>()
    val minimumMigrationVersion = "1.2.3"
    val timeoutMs = 500L
    val check = createConfigsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs)

    assertNotNull(check)
    val configsDatabaseMigrationCheck = assertInstanceOf(ConfigsDatabaseMigrationCheck::class.java, check)
    assertNotNull(configsDatabaseMigrationCheck.databaseAvailabilityCheck)
    assertInstanceOf(ConfigsDatabaseAvailabilityCheck::class.java, configsDatabaseMigrationCheck.databaseAvailabilityCheck)
    assertEquals(minimumMigrationVersion, configsDatabaseMigrationCheck.minimumFlywayVersion)
    assertEquals(timeoutMs, configsDatabaseMigrationCheck.timeoutMs)
    assertNotNull(configsDatabaseMigrationCheck.flyway)
    assertEquals(flyway, configsDatabaseMigrationCheck.flyway)
  }

  @Test
  fun testCreateJobsDatabaseMigrationCheck() {
    val dslContext = mockk<DSLContext>()
    val flyway = mockk<Flyway>()
    val minimumMigrationVersion = "1.2.3"
    val timeoutMs = 500L
    val check = createJobsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs)

    assertNotNull(check)
    val jobsDatabaseMigrationCheck = assertInstanceOf(JobsDatabaseMigrationCheck::class.java, check)
    assertNotNull(jobsDatabaseMigrationCheck.databaseAvailabilityCheck)
    assertInstanceOf(JobsDatabaseAvailabilityCheck::class.java, jobsDatabaseMigrationCheck.databaseAvailabilityCheck)
    assertEquals(minimumMigrationVersion, jobsDatabaseMigrationCheck.minimumFlywayVersion)
    assertEquals(timeoutMs, jobsDatabaseMigrationCheck.timeoutMs)
    assertNotNull(jobsDatabaseMigrationCheck.flyway)
    assertEquals(flyway, jobsDatabaseMigrationCheck.flyway)
  }

  @Test
  fun testCreateConfigsDatabaseInitializer() {
    val dslContext = mockk<DSLContext>()
    val initialSchema = "SELECT 1;"
    val timeoutMs = 500L
    val initializer = createConfigsDatabaseInitializer(dslContext, timeoutMs, initialSchema)

    assertNotNull(initializer)
    val configsDatabaseInitializer = assertInstanceOf(ConfigsDatabaseInitializer::class.java, initializer)
    assertNotNull(configsDatabaseInitializer.databaseAvailabilityCheck)
    assertInstanceOf(ConfigsDatabaseAvailabilityCheck::class.java, configsDatabaseInitializer.databaseAvailabilityCheck)
    assertEquals(timeoutMs, configsDatabaseInitializer.databaseAvailabilityCheck.timeoutMs)
    assertNotNull(configsDatabaseInitializer.dslContext)
    assertEquals(dslContext, configsDatabaseInitializer.dslContext)
    assertEquals(initialSchema, configsDatabaseInitializer.initialSchema)
  }

  @Test
  fun testCreateJobsDatabaseInitializer() {
    val dslContext = mockk<DSLContext>()
    val initialSchema = "SELECT 1;"
    val timeoutMs = 500L
    val initializer = createJobsDatabaseInitializer(dslContext, timeoutMs, initialSchema)

    assertNotNull(initializer)
    val jobsDatabaseInitializer = assertInstanceOf(JobsDatabaseInitializer::class.java, initializer)
    assertNotNull(jobsDatabaseInitializer.databaseAvailabilityCheck)
    assertInstanceOf(JobsDatabaseAvailabilityCheck::class.java, jobsDatabaseInitializer.databaseAvailabilityCheck)
    assertEquals(timeoutMs, jobsDatabaseInitializer.databaseAvailabilityCheck.timeoutMs)
    assertNotNull(jobsDatabaseInitializer.dslContext)
    assertEquals(dslContext, jobsDatabaseInitializer.dslContext)
    assertEquals(initialSchema, jobsDatabaseInitializer.initialSchema)
  }
}
