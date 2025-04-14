/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs

import io.airbyte.db.Database
import io.airbyte.db.instance.FlywayDatabaseMigrator
import org.flywaydb.core.Flyway

/**
 * Config db migrator.
 */
class ConfigsDatabaseMigrator(
  database: Database,
  flyway: Flyway,
) : FlywayDatabaseMigrator(database = database, flyway = flyway) {
  companion object {
    const val DB_IDENTIFIER: String = "configs"
    const val MIGRATION_FILE_LOCATION: String = "classpath:io/airbyte/db/instance/configs/migrations"
  }
}
