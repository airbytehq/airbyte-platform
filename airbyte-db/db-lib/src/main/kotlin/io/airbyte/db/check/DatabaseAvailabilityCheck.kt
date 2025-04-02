/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.airbyte.db.Database
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.slf4j.Logger
import java.util.function.Function

/**
 * The number of times to check if the database is available.
 * TODO replace with a default value in a value injection annotation
 */
private const val NUM_POLL_TIMES: Int = 10

/**
 * Performs a check to verify that the configured database is available.
 */
interface DatabaseAvailabilityCheck {
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
   * Retrieves the configured [Logger] object to be used to record progress of the migration
   * check.
   *
   * @return The configured [Logger] object.
   */
  fun getLogger(): Logger

  /**
   * Retrieves the timeout in milliseconds for the check. Once this timeout is exceeded, the check
   * will fail with an [InterruptedException].
   *
   * @return The timeout in milliseconds for the check.
   */
  fun getTimeoutMs(): Long

  /**
   * Checks whether the configured database is available.
   *
   * @throws DatabaseCheckException if unable to perform the check.
   */
  @Throws(DatabaseCheckException::class)
  fun check() {
    var initialized = false
    var totalTime = 0
    val sleepTime: Long = getTimeoutMs() / NUM_POLL_TIMES

    while (!initialized) {
      getLogger().warn("Waiting for database to become available...")
      if (totalTime >= getTimeoutMs()) {
        throw DatabaseCheckException("Unable to connect to the database.")
      }

      getDslContext()?.let { dslContext ->
        val database = Database(dslContext)
        initialized = isDatabaseConnected(getDatabaseName())(database)
        if (!initialized) {
          getLogger().info("Database is not ready yet. Please wait a moment, it might still be initializing...")
          try {
            Thread.sleep(sleepTime)
          } catch (e: InterruptedException) {
            throw DatabaseCheckException("Unable to wait for database to be ready.", e)
          }
          totalTime += sleepTime.toInt()
        } else {
          getLogger().info("Database available.")
        }
      } ?: throw DatabaseCheckException("Database configuration not present.")
    }
  }

  /**
   * Generates a [Function] that is used to test if a connection can be made to the database by
   * verifying that the `information_schema.tables` tables has been populated.
   *
   * @param databaseName The name of the database to test.
   * @return A [Function] that can be invoked to test if the database is available.
   */
  fun isDatabaseConnected(databaseName: String?): (Database) -> Boolean =
    { database: Database ->
      try {
        getLogger().info("Testing {} database connection...", databaseName)
        database.query<Boolean> { ctx: DSLContext ->
          ctx.fetchExists(
            DSL.select().from("information_schema.tables"),
          )
        }
      } catch (e: Exception) {
        getLogger().error("Failed to verify database connection.", e)
        false
      }
    }
}
