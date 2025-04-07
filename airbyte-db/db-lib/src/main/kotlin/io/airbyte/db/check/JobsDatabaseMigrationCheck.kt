/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway

private val log = KotlinLogging.logger {}

/**
 * Implementation of the [DatabaseMigrationCheck] for the Jobs database.
 *
 * TODO inject via dependency injection framework
 */
class JobsDatabaseMigrationCheck(
  override val databaseAvailabilityCheck: JobsDatabaseAvailabilityCheck,
  override val flyway: Flyway,
  override val minimumFlywayVersion: String,
  override val timeoutMs: Long,
) : DatabaseMigrationCheck {
  override val log = io.airbyte.db.check.log
}
