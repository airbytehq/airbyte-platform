/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.mockk
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class DatabaseInitializerTest {
  @Test
  fun testExceptionHandling() {
    val initializer: DatabaseInitializer =
      object : DatabaseInitializer {
        override fun initialize(): Unit = throw DatabaseInitializationException("test")

        override val databaseAvailabilityCheck = null

        override val databaseName = "name"

        override val dslContext: DSLContext? = null

        override val initialSchema = "schema"

        override val log = KotlinLogging.logger {}

        override val tableNames = emptySet<String>()
      }

    assertThrows<DatabaseInitializationException> { initializer.initialize() }
  }

  @Test
  fun testEmptyTableNames() {
    val dslContext = mockk<DSLContext>()
    val initializer: DatabaseInitializer =
      object : DatabaseInitializer {
        override val databaseAvailabilityCheck = mockk<DatabaseAvailabilityCheck>()

        override val databaseName = "name"

        override val dslContext: DSLContext = dslContext

        override val initialSchema = "schema"

        override val log = KotlinLogging.logger {}

        override val tableNames = emptySet<String>()
      }

    assertFalse(initializer.initializeSchema(dslContext))
    assertNotNull(initializer.tableNames)
    assertTrue(initializer.tableNames.isEmpty())
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
