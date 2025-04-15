/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs

import io.airbyte.commons.resources.Resources
import io.airbyte.db.Database
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createConfigsDatabaseInitializer
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.db.instance.FlywayMigrationDatabase
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import java.io.IOException

/**
 * Configs database for jOOQ code generation.
 */
class ConfigsFlywayMigrationDatabase : FlywayMigrationDatabase() {
  @Throws(IOException::class)
  override fun getDatabase(dslContext: DSLContext): Database = Database(dslContext)

  override fun getDatabaseMigrator(
    database: Database,
    flyway: Flyway,
  ): DatabaseMigrator = ConfigsDatabaseMigrator(database, flyway)

  override val installedBy: String
    get() = ConfigsFlywayMigrationDatabase::class.java.simpleName

  override val dbIdentifier: String
    get() = ConfigsDatabaseMigrator.DB_IDENTIFIER

  override val migrationFileLocations: Array<String>
    get() = arrayOf(ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION)

  @Throws(DatabaseInitializationException::class, IOException::class)
  override fun initializeDatabase(dslContext: DSLContext) {
    val initialSchema = Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH)
    createConfigsDatabaseInitializer(
      dslContext,
      DatabaseConstants.DEFAULT_CONNECTION_TIMEOUT_MS,
      initialSchema,
    ).initialize()
  }
}
