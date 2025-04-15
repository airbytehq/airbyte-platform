/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck
import io.airbyte.db.check.ConfigsDatabaseMigrationCheck
import io.airbyte.db.check.DatabaseMigrationCheck
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.check.JobsDatabaseMigrationCheck
import io.airbyte.db.init.ConfigsDatabaseInitializer
import io.airbyte.db.init.DatabaseInitializer
import io.airbyte.db.init.JobsDatabaseInitializer
import org.flywaydb.core.Flyway
import org.jooq.DSLContext

/**
 * Temporary factory class that provides convenience methods for creating a
 * [io.airbyte.db.check.DatabaseCheck] and [DatabaseInitializer] instances. This class
 * will be removed once the project has been converted to leverage an application framework to
 * manage the creation and injection of various check objects.
 */
open class DatabaseCheckFactory {
  companion object {
    /**
     * Constructs a new [DatabaseAvailabilityCheck] that verifies the availability of the
     * `Configurations` database.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the
     * `Configurations` database.
     * @param timeoutMs The amount of time to wait for the database to become available, in
     * milliseconds.
     * @return A configured [DatabaseAvailabilityCheck] for the `Configurations` database.
     */
    @JvmStatic
    fun createConfigsDatabaseAvailabilityCheck(
      dslContext: DSLContext,
      timeoutMs: Long,
    ): ConfigsDatabaseAvailabilityCheck = ConfigsDatabaseAvailabilityCheck(dslContext, timeoutMs)

    /**
     * Constructs a new [DatabaseAvailabilityCheck] that verifies the availability of the `Jobs` database.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the `Jobs` database.
     * @param timeoutMs The amount of time to wait for the database to become available, in
     * milliseconds.
     * @return A configured [DatabaseAvailabilityCheck] for the `Jobs` database.
     */
    @JvmStatic
    fun createJobsDatabaseAvailabilityCheck(
      dslContext: DSLContext,
      timeoutMs: Long,
    ): JobsDatabaseAvailabilityCheck = JobsDatabaseAvailabilityCheck(dslContext, timeoutMs)

    /**
     * Constructs a new [DatabaseMigrationCheck] that verifies that the `Configurations` database has been migrated to the requested minimum schema version.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the `Configurations` database.
     * @param flyway The [Flyway] instance used to determine the current migration status.
     * @param minimumMigrationVersion The required minimum schema version.
     * @param timeoutMs Teh amount of time to wait for the migration to complete/match the requested minimum schema version, in milliseconds.
     * @return The configured [DatabaseMigrationCheck] for the `Configurations` database.
     */
    @JvmStatic
    fun createConfigsDatabaseMigrationCheck(
      dslContext: DSLContext,
      flyway: Flyway,
      minimumMigrationVersion: String,
      timeoutMs: Long,
    ): DatabaseMigrationCheck =
      ConfigsDatabaseMigrationCheck(
        createConfigsDatabaseAvailabilityCheck(dslContext, timeoutMs),
        flyway,
        minimumMigrationVersion,
        timeoutMs,
      )

    /**
     * Constructs a new [DatabaseMigrationCheck] that verifies that the `Jobs` database has been migrated to the requested minimum schema version.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the `Configurations` database.
     * @param flyway The [Flyway] instance used to determine the current migration status.
     * @param minimumMigrationVersion The required minimum schema version.
     * @param timeoutMs Teh amount of time to wait for the migration to complete/match the requested minimum schema version, in milliseconds.
     * @return The configured [DatabaseMigrationCheck] for the `Jobs` database.
     */
    @JvmStatic
    fun createJobsDatabaseMigrationCheck(
      dslContext: DSLContext,
      flyway: Flyway,
      minimumMigrationVersion: String,
      timeoutMs: Long,
    ): DatabaseMigrationCheck =
      JobsDatabaseMigrationCheck(
        createJobsDatabaseAvailabilityCheck(dslContext, timeoutMs),
        flyway,
        minimumMigrationVersion,
        timeoutMs,
      )

    /**
     * Constructs a new [DatabaseInitializer] that ensures that the `Configurations` database schema has been initialized.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the `Configurations` database.
     * @param timeoutMs The amount of time to wait for the database to become available, in milliseconds.
     * @param initialSchema The initial schema creation script to be executed if the database is not already populated.
     * @return The configured [DatabaseInitializer] for the `Configurations` database.
     */
    @JvmStatic
    fun createConfigsDatabaseInitializer(
      dslContext: DSLContext,
      timeoutMs: Long,
      initialSchema: String,
    ): DatabaseInitializer =
      ConfigsDatabaseInitializer(
        createConfigsDatabaseAvailabilityCheck(dslContext, timeoutMs),
        dslContext,
        initialSchema,
      )

    /**
     * Constructs a new [DatabaseInitializer] that ensures that the `Jobs` database schema has been initialized.
     *
     * @param dslContext The [DSLContext] instance used to communicate with the `Jobs` database.
     * @param timeoutMs The amount of time to wait for the database to become available, in milliseconds.
     * @param initialSchema The initial schema creation script to be executed if the database is not already populated.
     * @return The configured [DatabaseInitializer] for the `Jobs` database.
     */
    @JvmStatic
    fun createJobsDatabaseInitializer(
      dslContext: DSLContext,
      timeoutMs: Long,
      initialSchema: String,
    ): DatabaseInitializer =
      JobsDatabaseInitializer(
        createJobsDatabaseAvailabilityCheck(dslContext, timeoutMs),
        dslContext,
        initialSchema,
      )
  }
}
