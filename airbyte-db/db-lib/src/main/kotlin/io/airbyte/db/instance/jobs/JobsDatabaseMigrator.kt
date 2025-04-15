/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs

import io.airbyte.db.Database
import io.airbyte.db.instance.FlywayDatabaseMigrator
import org.flywaydb.core.Flyway

/**
 * Migrator for jobs db.
 */
class JobsDatabaseMigrator(
  database: Database,
  flyway: Flyway,
) : FlywayDatabaseMigrator(database, flyway) {
  companion object {
    const val DB_IDENTIFIER: String = "jobs"
    const val MIGRATION_FILE_LOCATION: String = "classpath:io/airbyte/db/instance/jobs/migrations"
  }
}
