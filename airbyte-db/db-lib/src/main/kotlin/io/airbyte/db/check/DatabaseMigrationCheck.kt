/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import org.flywaydb.core.Flyway
import org.slf4j.Logger

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
   * Retrieves the [DatabaseAvailabilityCheck] used to verify that the database is running and
   * available.
   *
   * @return The [DatabaseAvailabilityCheck].
   */
  fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck?

  /**
   * Retrieves the configured [Flyway] object to be used to check the migration status of the
   * database.
   *
   * @return The configured [Flyway] object.
   */
  fun getFlyway(): Flyway?

  /**
   * Retrieves the configured [Logger] object to be used to record progress of the migration
   * check.
   *
   * @return The configured [Logger] object.
   */
  fun getLogger(): Logger

  /**
   * Retrieves the required minimum migration version of the schema.
   *
   * @return The required minimum migration version of the schema.
   */
  fun getMinimumFlywayVersion(): String

  /**
   * Retrieves the timeout in milliseconds for the check. Once this timeout is exceeded, the check
   * will fail with an [InterruptedException].
   *
   * @return The timeout in milliseconds for the check.
   */
  fun getTimeoutMs(): Long

  /**
   * Checks whether the configured database has been migrated to the required minimum schema version.
   *
   * @throws DatabaseCheckException if unable to perform the check.
   */
  @Throws(DatabaseCheckException::class)
  fun check() {
    val startTime = System.currentTimeMillis()
    val sleepTime: Long = getTimeoutMs() / NUM_POLL_TIMES
    val flyway = getFlyway()

    // Verify that the database is up and reachable first
    val availabilityCheck =
      getDatabaseAvailabilityCheck()?.let { availabilityCheck ->
        availabilityCheck.check()

        flyway?.let { flyway ->
          var currDatabaseMigrationVersion = getCurrentVersion(flyway)
          getLogger().info("Current database migration version {}.", currDatabaseMigrationVersion)
          getLogger().info("Minimum Flyway version required {}.", getMinimumFlywayVersion())

          while (currDatabaseMigrationVersion.compareTo(getMinimumFlywayVersion()!!) < 0) {
            if (System.currentTimeMillis() - startTime >= getTimeoutMs()) {
              throw DatabaseCheckException("Timeout while waiting for database to fulfill minimum flyway migration version..")
            }

            try {
              Thread.sleep(sleepTime)
            } catch (e: InterruptedException) {
              throw DatabaseCheckException("Unable to wait for database to be migrated.", e)
            }

            currDatabaseMigrationVersion = getCurrentVersion(flyway)
          }
          getLogger().info("Verified that database has been migrated to the required minimum version {}.", getMinimumFlywayVersion())
        } ?: throw DatabaseCheckException("Flyway configuration not present.")
      } ?: throw DatabaseCheckException("Availability check not configured.")
  }

  /**
   * Retrieves the current version of the migration schema.
   *
   * @param flyway A [Flyway] that can be used to retrieve the current version.
   * @return The current version of the migrated schema or [.UNAVAILABLE_VERSION] if the version
   * cannot be discovered.
   */
  fun getCurrentVersion(flyway: Flyway): String {
    /*
     * The database may be available, but not yet migrated. If this is the case, the Flyway object will
     * not be able to retrieve the current version of the schema. If that happens, return a fake version
     * so that the check will fail and try again.
     */
    return if (flyway.info().current() != null) {
      flyway
        .info()
        .current()
        .version.version
    } else {
      UNAVAILABLE_VERSION
    }
  }
}
