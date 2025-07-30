/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.airbyte.db.Database
import io.github.oshai.kotlinlogging.KLogger
import org.jooq.DSLContext
import org.jooq.impl.DSL
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
  val databaseName: String

  /**
   * Retrieves the configured [DSLContext] to be used to test the database availability.
   *
   * @return The configured [DSLContext] object.
   */
  val dslContext: DSLContext

  /**
   * Retrieves the configured [Logger] object to be used to record progress of the migration
   * check.
   *
   * @return The configured [Logger] object.
   */
  val log: KLogger

  /**
   * Retrieves the timeout in milliseconds for the check. Once this timeout is exceeded, the check
   * will fail with an [InterruptedException].
   *
   * @return The timeout in milliseconds for the check.
   */
  val timeoutMs: Long

  /**
   * Checks whether the configured database is available.
   *
   * @throws DatabaseCheckException if unable to perform the check.
   */
  @Throws(DatabaseCheckException::class)
  fun check() {
    var initialized = false
    var totalTime = 0
    val sleepTime: Long = timeoutMs / NUM_POLL_TIMES

    while (!initialized) {
      log.warn { "Waiting for database to become available..." }
      if (totalTime >= timeoutMs) {
        throw DatabaseCheckException("Unable to connect to the database.")
      }

      val database = Database(dslContext)
      initialized = isDatabaseConnected(databaseName)(database)
      if (!initialized) {
        log.info { "Database is not ready yet. Please wait a moment, it might still be initializing..." }
        try {
          Thread.sleep(sleepTime)
        } catch (e: InterruptedException) {
          throw DatabaseCheckException("Unable to wait for database to be ready.", e)
        }
        totalTime += sleepTime.toInt()
      } else {
        log.info { "Database available." }
      }
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
        log.info { "Testing $databaseName database connection..." }
        database.query { ctx: DSLContext ->
          ctx.fetchExists(
            DSL.select().from("information_schema.tables"),
          )
        }
      } catch (e: Exception) {
        log.error(e) { "Failed to verify database connection." }
        false
      }
    }
}
