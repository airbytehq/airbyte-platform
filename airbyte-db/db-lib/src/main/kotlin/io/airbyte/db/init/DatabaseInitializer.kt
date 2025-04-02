/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.airbyte.db.check.DatabaseCheckException
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.slf4j.Logger
import java.io.IOException

/**
 * Performs the initialization of the configured database if the database is available and has not
 * yet been initialized.
 *
 * In the future, this logic could be completely removed if the schema initialization script is
 * converted to a migration script.
 */
interface DatabaseInitializer {
  /**
   * Retrieves the [DatabaseAvailabilityCheck] used to verify that the database is running and
   * available.
   *
   * @return The [DatabaseAvailabilityCheck].
   */
  fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck?

  /**
   * Retrieves the configured database name to be tested.
   *
   * @return The name of the database to test.
   */
  fun getDatabaseName(): String

  /**
   * Retrieves the configured [DSLContext] to be used to test the database availability.
   *
   * @return The configured [DSLContext] object.
   */
  fun getDslContext(): DSLContext?

  /**
   * Retrieve the initial schema to be applied to the database if the database is not already
   * populated with the expected table(s).
   *
   * @return The initial schema.
   */
  fun getInitialSchema(): String

  /**
   * Retrieves the configured [Logger] object to be used to record progress of the migration
   * check.
   *
   * @return The configured [Logger] object.
   */
  fun getLogger(): Logger

  /**
   * The collection of table names that will be used to confirm database availability.
   *
   * @return The collection of database table names.
   */
  fun getTableNames(): Collection<String>

  /**
   * Initializes the configured database by using the following steps.
   *
   *
   *  1. Verify that the database is available and accepting connections
   *  1. Verify that the database is populated with the initial schema. If not, create the initial
   * schema.
   *
   *
   * @throws DatabaseInitializationException if unable to verify the database availability.
   */
  @Throws(DatabaseInitializationException::class)
  fun initialize() {
    // Verify that the database is up and reachable first
    val availabilityCheck = getDatabaseAvailabilityCheck() ?: throw DatabaseInitializationException("Availability check not configured.")
    try {
      availabilityCheck.check()
      val dslContext = getDslContext() ?: throw DatabaseInitializationException("Database configuration not present.")
      val database = Database(dslContext)
      ExceptionWrappingDatabase(database).transaction { ctx: DSLContext -> this.initializeSchema(ctx) }
    } catch (e: DatabaseCheckException) {
      throw DatabaseInitializationException("Database availability check failed.", e)
    } catch (e: IOException) {
      throw DatabaseInitializationException("Database availability check failed.", e)
    }
  }

  /**
   * Tests whether the provided table exists in the database.
   *
   * @param ctx A [DSLContext] used to query the database.
   * @param tableName The name of the table.
   * @return `True` if the table exists or `false` otherwise.
   */
  fun hasTable(
    ctx: DSLContext,
    tableName: String?,
  ): Boolean =
    ctx.fetchExists(
      DSL
        .select()
        .from("information_schema.tables")
        .where(
          DSL
            .field("table_name")
            .eq(tableName)
            .and(DSL.field("table_schema").eq("public")),
        ),
    )

  /**
   * Initializes the schema in the database represented by the provided [DSLContext] instance.
   *
   * If the initial tables already exist in the database, initialization is skipped. Otherwise, the
   * script provided by the [.getInitialSchema] method is executed against the database.
   *
   * @param ctx The [DSLContext] used to execute the schema initialization.
   * @return `true` indicating that the operation ran
   */
  fun initializeSchema(ctx: DSLContext): Boolean {
    val tableNames = getTableNames()
    if (tableNames.isEmpty()) {
      getLogger().warn("Initial collection of table names is empty.  Cannot perform schema check.")
      return false
    }

    val dbName = getDatabaseName()

    // Verify that all the required tables are present
    if (tableNames.all { hasTable(ctx, it) }) {
      getLogger().info("The $dbName database is initialized")
    } else {
      val initSchema = getInitialSchema()
      getLogger().info(
        "The $dbName database has not been initialized; initializing it with schema: \n$initSchema",
      )

      ctx.execute(getInitialSchema())
      getLogger().info("The $dbName database successfully initialized with schema: \n$initSchema.")
    }
    return true
  }
}
