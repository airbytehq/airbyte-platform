/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.test

import io.airbyte.db.Database
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.ConfigsDatabaseTestProvider
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator
import io.airbyte.db.instance.jobs.JobsDatabaseTestProvider
import org.jooq.DSLContext
import java.io.IOException
import javax.sql.DataSource

/**
 * Use this class to create mock databases in unit tests. This class takes care of database
 * initialization and migration.
 */
class TestDatabaseProviders(
  private val dataSource: DataSource,
  private val dslContext: DSLContext,
) {
  private var runMigration = true

  /**
   * When creating mock databases in unit tests, migration should be run by default. Call this method
   * to turn migration off, which is needed when unit testing migration code.
   */
  fun turnOffMigration() = apply { runMigration = false }

  /**
   * Create new configs db.
   *
   * @return configs db.
   * @throws IOException exception while accessing db
   * @throws DatabaseInitializationException exception while initializing db
   */
  @Throws(IOException::class, DatabaseInitializationException::class)
  fun createNewConfigsDatabase(): Database {
    val flyway =
      create(
        dataSource,
        ConfigsDatabaseTestProvider::class.java.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    return ConfigsDatabaseTestProvider(dslContext, flyway).create(runMigration)
  }

  /**
   * Create new jobs db.
   *
   * @return jobs db.
   * @throws IOException exception while accessing db
   * @throws DatabaseInitializationException exception while initializing db
   */
  @Throws(IOException::class, DatabaseInitializationException::class)
  fun createNewJobsDatabase(): Database {
    val flyway =
      create(
        dataSource,
        JobsDatabaseTestProvider::class.java.simpleName,
        JobsDatabaseMigrator.DB_IDENTIFIER,
        JobsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    return JobsDatabaseTestProvider(dslContext, flyway).create(runMigration)
  }
}
