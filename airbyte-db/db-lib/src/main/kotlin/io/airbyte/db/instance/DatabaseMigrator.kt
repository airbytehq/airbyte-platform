/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance

import org.flywaydb.core.api.MigrationInfo
import org.flywaydb.core.api.output.BaselineResult
import org.flywaydb.core.api.output.MigrateResult

/**
 * Database migrator interface.
 */
interface DatabaseMigrator {
  /**
   * Run migration.
   */
  fun migrate(): MigrateResult?

  /**
   * List migration information.
   */
  fun list(): List<MigrationInfo>

  /**
   * Get the latest migration information.
   */
  fun getLatestMigration(): MigrationInfo?

  /**
   * Setup Flyway migration in a database and create baseline.
   */
  fun createBaseline(): BaselineResult?

  /**
   * Dump the current database schema.
   */
  fun dumpSchema(): String
}
