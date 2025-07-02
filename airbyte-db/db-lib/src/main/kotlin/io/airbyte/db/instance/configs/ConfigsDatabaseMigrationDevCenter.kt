/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs

import io.airbyte.db.Database
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.FlywayDatabaseMigrator
import io.airbyte.db.instance.development.MigrationDevCenter
import org.flywaydb.core.Flyway
import javax.sql.DataSource

/**
 * Helper class for migration development. See README for details.
 */
class ConfigsDatabaseMigrationDevCenter :
  MigrationDevCenter(
    dbIdentifier = "configs",
    schemaDumpFile = DatabaseConstants.CONFIGS_SCHEMA_DUMP_PATH,
    initialScript = DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH,
  ) {
  override fun getMigrator(
    database: Database,
    flyway: Flyway,
  ): FlywayDatabaseMigrator = ConfigsDatabaseMigrator(database!!, flyway)

  override fun getFlyway(dataSource: DataSource): Flyway =
    create(
      dataSource,
      javaClass.simpleName,
      ConfigsDatabaseMigrator.DB_IDENTIFIER,
      ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
    )
}
