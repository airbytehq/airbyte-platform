/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.instance.DatabaseConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext

private val log = KotlinLogging.logger {}

/**
 * Implementation of the [DatabaseInitializer] for the Jobs database that creates the schema
 * if it does not currently exist.
 *
 * TODO inject via dependency injection framework
 */
class JobsDatabaseInitializer(
  override val databaseAvailabilityCheck: JobsDatabaseAvailabilityCheck,
  override val dslContext: DSLContext,
  override val initialSchema: String,
) : DatabaseInitializer {
  override val databaseName = DatabaseConstants.JOBS_DATABASE_LOGGING_NAME

  override val log = io.airbyte.db.init.log

  override val tableNames = setOf("jobs", "airbyte_metadata", "attempts")
}
