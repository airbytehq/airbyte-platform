/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import org.flywaydb.core.Flyway
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Implementation of the [DatabaseMigrationCheck] for the Jobs database.
 */
class JobsDatabaseMigrationCheck(
// TODO inject via dependency injection framework
  private val databaseAvailablityCheck: JobsDatabaseAvailabilityCheck,
  // TODO inject via dependency injection framework
  private val flyway: Flyway,
  // TODO inject via dependency injection framework
  private val minimumFlywayVersion: String,
  // TODO inject via dependency injection framework
  private val timeoutMs: Long,
) : DatabaseMigrationCheck {
  override fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck? = databaseAvailablityCheck

  override fun getFlyway(): Flyway? = flyway

  override fun getLogger(): Logger = LOGGER

  override fun getMinimumFlywayVersion(): String = minimumFlywayVersion

  override fun getTimeoutMs(): Long = timeoutMs

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(JobsDatabaseMigrationCheck::class.java)
  }
}
