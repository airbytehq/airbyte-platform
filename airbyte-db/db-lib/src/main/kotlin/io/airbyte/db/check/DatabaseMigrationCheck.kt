/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.github.oshai.kotlinlogging.KLogger
import org.flywaydb.core.Flyway

/**
 * Represents an unavailable schema migration version that ensures a re-test.
 */
private const val UNAVAILABLE_VERSION = "0"

/**
 * The number of times to check if the database has been migrated to the required schema version.
 * TODO replace with a default value in a value injection annotation
 */
private const val NUM_POLL_TIMES = 10

/**
 * Performs a check to verify that the configured database has been migrated to the appropriate version.
 */
interface DatabaseMigrationCheck {
  /**
   * Retrieves the configured [Logger] object to be used to record progress of the migration
   * check.
   *
   * @return The configured [Logger] object.
   */
  val log: KLogger

  /**
   * Retrieves the [DatabaseAvailabilityCheck] used to verify that the database is running and
   * available.
   *
   * @return The [DatabaseAvailabilityCheck].
   */
  val databaseAvailabilityCheck: DatabaseAvailabilityCheck

  /**
   * Retrieves the configured [Flyway] object to be used to check the migration status of the
   * database.
   *
   * @return The configured [Flyway] object.
   */
  val flyway: Flyway

  /**
   * Retrieves the required minimum migration version of the schema.
   *
   * @return The required minimum migration version of the schema.
   */
  val minimumFlywayVersion: String

  /**
   * Retrieves the timeout in milliseconds for the check. Once this timeout is exceeded, the check
   * will fail with an [InterruptedException].
   *
   * @return The timeout in milliseconds for the check.
   */
  val timeoutMs: Long

  /**
   * Checks whether the configured database has been migrated to the required minimum schema version.
   *
   * @throws DatabaseCheckException if unable to perform the check.
   */
  @Throws(DatabaseCheckException::class)
  fun check() {
    val startTime = System.currentTimeMillis()
    val sleepTime: Long = timeoutMs / NUM_POLL_TIMES

    // Verify that the database is up and reachable first
    databaseAvailabilityCheck.let { availabilityCheck ->
      availabilityCheck.check()

      var currDatabaseMigrationVersion = flyway.currentVersion()
      log.info { "Current database migration version $currDatabaseMigrationVersion." }
      log.info { "Minimum Flyway version required $minimumFlywayVersion." }

      while (currDatabaseMigrationVersion.compareTo(minimumFlywayVersion) < 0) {
        if (System.currentTimeMillis() - startTime >= timeoutMs) {
          throw DatabaseCheckException("Timeout while waiting for database to fulfill minimum flyway migration version..")
        }

        try {
          Thread.sleep(sleepTime)
        } catch (e: InterruptedException) {
          throw DatabaseCheckException("Unable to wait for database to be migrated.", e)
        }

        currDatabaseMigrationVersion = flyway.currentVersion()
      }
      log.info { "Verified that database has been migrated to the required minimum version $minimumFlywayVersion." }
    }
  }
}

/**
 * Retrieves the current version of the migration schema.
 *
 * @receiver flyway A [Flyway] that can be used to retrieve the current version.
 * @return The current version of the migrated schema or [UNAVAILABLE_VERSION] if the version cannot be discovered.
 */
private fun Flyway.currentVersion(): String = info().current()?.version?.version ?: UNAVAILABLE_VERSION
