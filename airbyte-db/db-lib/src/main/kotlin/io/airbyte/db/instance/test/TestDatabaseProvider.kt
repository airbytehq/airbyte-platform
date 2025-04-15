/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.test

import io.airbyte.db.Database
import io.airbyte.db.init.DatabaseInitializationException
import java.io.IOException

/**
 * Create mock database in unit tests. The implementation will be responsible for: 1) constructing
 * and preparing the database, and 2) running the Flyway migration.
 */
interface TestDatabaseProvider {
  /**
   * Create test database.
   *
   * @param runMigration Whether the mock database should run Flyway migration before it is used in
   * unit test. Usually this parameter should be false only when the migration itself is being
   * tested.
   */
  @Throws(IOException::class, DatabaseInitializationException::class)
  fun create(runMigration: Boolean): Database
}
