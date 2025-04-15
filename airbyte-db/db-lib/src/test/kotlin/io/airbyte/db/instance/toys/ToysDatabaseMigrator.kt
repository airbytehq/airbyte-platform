/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

import io.airbyte.db.Database
import io.airbyte.db.instance.FlywayDatabaseMigrator
import org.flywaydb.core.Flyway

/**
 * A database migrator for testing purposes only.
 */
class ToysDatabaseMigrator(
  database: Database,
  flyway: Flyway,
) : FlywayDatabaseMigrator(database, flyway) {
  companion object {
    const val DB_IDENTIFIER: String = "toy"
    const val MIGRATION_FILE_LOCATION: String = "classpath:io/airbyte/db/instance/toys/migrations"
  }
}
