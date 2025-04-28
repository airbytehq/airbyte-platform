/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

import io.airbyte.commons.resources.Resources
import io.airbyte.db.Database
import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.test.utils.AbstractDatabaseTest
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import javax.sql.DataSource

private const val PRE_MIGRATION_SCHEMA_DUMP = "toys_database/pre_migration_schema.txt"
private const val POST_MIGRATION_SCHEMA_DUMP = "toys_database/schema_dump.txt"

internal class ToysDatabaseMigratorTest : AbstractDatabaseTest() {
  override fun getDatabase(
    dataSource: DataSource,
    dslContext: DSLContext,
  ): Database = Database(dslContext)

  @Test
  fun testMigration() {
    val dataSource = getDataSource()

    initializeDatabase(getDslContext())

    val flyway =
      create(
        dataSource,
        javaClass.simpleName,
        ToysDatabaseMigrator.DB_IDENTIFIER,
        ToysDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val migrator: DatabaseMigrator = ToysDatabaseMigrator(database, flyway)

    // Compare pre migration baseline schema
    migrator.createBaseline()
    val preMigrationSchema = Resources.read(PRE_MIGRATION_SCHEMA_DUMP).trim()
    val actualPreMigrationSchema = migrator.dumpSchema()
    assertEquals(
      preMigrationSchema,
      actualPreMigrationSchema,
      "The pre migration schema dump has changed",
    )

    // Compare post migration schema
    migrator.migrate()
    val postMigrationSchema = Resources.read(POST_MIGRATION_SCHEMA_DUMP).trim()
    val actualPostMigrationSchema = migrator.dumpSchema()
    assertEquals(
      postMigrationSchema,
      actualPostMigrationSchema,
      "The post migration schema dump has changed",
    )
  }

  @Throws(DatabaseInitializationException::class, IOException::class)
  private fun initializeDatabase(dslContext: DSLContext) {
    val initialSchema = Resources.read(ToysDatabaseConstants.SCHEMA_PATH)
    val availabilityCheck: DatabaseAvailabilityCheck =
      ToysDatabaseAvailabilityCheck(
        dslContext,
        DatabaseConstants.DEFAULT_CONNECTION_TIMEOUT_MS,
      )
    ToysDatabaseInitializer(availabilityCheck, dslContext, initialSchema).initialize()
  }
}
