/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs

import io.airbyte.commons.resources.Resources
import io.airbyte.db.Database
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createJobsDatabaseInitializer
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.db.instance.FlywayMigrationDatabase
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import java.io.IOException

/**
 * Jobs database for jOOQ code generation.
 */
class JobsFlywayMigrationDatabase : FlywayMigrationDatabase() {
  override fun getDatabase(dslContext: DSLContext): Database = Database(dslContext)

  override fun getDatabaseMigrator(
    database: Database,
    flyway: Flyway,
  ): DatabaseMigrator = JobsDatabaseMigrator(database, flyway)

  override val installedBy: String
    get() = JobsFlywayMigrationDatabase::class.java.simpleName

  override val dbIdentifier: String
    get() = JobsDatabaseMigrator.DB_IDENTIFIER

  override val migrationFileLocations: Array<String>
    get() = arrayOf(JobsDatabaseMigrator.MIGRATION_FILE_LOCATION)

  @Throws(DatabaseInitializationException::class, IOException::class)
  override fun initializeDatabase(dslContext: DSLContext) {
    val initialSchema = Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH)
    createJobsDatabaseInitializer(
      dslContext,
      DatabaseConstants.DEFAULT_CONNECTION_TIMEOUT_MS,
      initialSchema,
    ).initialize()
  }
}
