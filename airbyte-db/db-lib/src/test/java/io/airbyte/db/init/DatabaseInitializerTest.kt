/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.check.DatabaseAvailabilityCheck
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class DatabaseInitializerTest {
  @Test
  fun testExceptionHandling() {
    val initializer: DatabaseInitializer =
      object : DatabaseInitializer {
        @Throws(DatabaseInitializationException::class)
        override fun initialize(): Unit = throw DatabaseInitializationException("test")

        override fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck? = null

        override fun getDatabaseName(): String = "name"

        override fun getDslContext(): DSLContext? = null

        override fun getInitialSchema(): String = "schema"

        override fun getLogger(): Logger = LOGGER

        override fun getTableNames(): Collection<String> = emptySet()
      }

    Assertions.assertThrows(
      DatabaseInitializationException::class.java,
    ) { initializer.initialize() }
  }

  @Test
  fun testEmptyTableNames() {
    val dslContext = Mockito.mock(DSLContext::class.java)
    val initializer: DatabaseInitializer =
      object : DatabaseInitializer {
        override fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck? = Mockito.mock(DatabaseAvailabilityCheck::class.java)

        override fun getDatabaseName(): String = "name"

        override fun getDslContext(): DSLContext? = dslContext

        override fun getInitialSchema(): String = "schema"

        override fun getLogger(): Logger = LOGGER

        override fun getTableNames(): Collection<String> = emptySet()
      }

    Assertions.assertFalse(initializer.initializeSchema(dslContext))
    Assertions.assertNotNull(initializer.getTableNames())
    Assertions.assertTrue(initializer.getTableNames().isEmpty())
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(DatabaseInitializerTest::class.java)
  }
}
