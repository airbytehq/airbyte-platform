/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs

import io.airbyte.commons.resources.Resources
import io.airbyte.db.Database
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createJobsDatabaseInitializer
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.db.instance.test.TestDatabaseProvider
import org.flywaydb.core.Flyway
import org.jooq.DSLContext

/**
 * Jobs db test provider.
 */
class JobsDatabaseTestProvider(
  private val dslContext: DSLContext,
  private val flyway: Flyway?,
) : TestDatabaseProvider {
  override fun create(runMigration: Boolean): Database {
    val initialSchema = Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH)
    createJobsDatabaseInitializer(
      dslContext,
      DatabaseConstants.DEFAULT_CONNECTION_TIMEOUT_MS,
      initialSchema,
    ).initialize()

    val jobsDatabase = Database(dslContext)

    if (runMigration) {
      val migrator: DatabaseMigrator =
        JobsDatabaseMigrator(
          jobsDatabase,
          flyway!!,
        )
      migrator.createBaseline()
      migrator.migrate()
    }

    return jobsDatabase
  }
}
