/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.instance.DatabaseConstants
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Common test suite for the classes found in the `io.airbyte.db.factory` package.
 */
internal open class CommonFactoryTest {
  companion object {
    private const val DATABASE_NAME = "airbyte_test_database"
    internal lateinit var container: PostgreSQLContainer<*>

    @BeforeAll
    @JvmStatic
    fun dbSetup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName(DATABASE_NAME)
          .withUsername("docker")
          .withPassword("docker")
          .apply { start() }
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }
  }
}
