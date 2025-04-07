/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck
import io.airbyte.db.instance.DatabaseConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext

private val log = KotlinLogging.logger {}

/**
 * Implementation of the [DatabaseInitializer] for the Configurations database that creates
 * the schema if it does not currently exist.
 *
 * TODO inject via dependency injection framework
 */
class ConfigsDatabaseInitializer(
  override val databaseAvailabilityCheck: ConfigsDatabaseAvailabilityCheck,
  override val dslContext: DSLContext,
  override val initialSchema: String,
) : DatabaseInitializer {
  override val databaseName = DatabaseConstants.CONFIGS_DATABASE_LOGGING_NAME

  override val log = io.airbyte.db.init.log

  override val tableNames = DatabaseConstants.CONFIGS_INITIAL_EXPECTED_TABLES
}
