/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck
import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.airbyte.db.instance.DatabaseConstants
import org.jooq.DSLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Implementation of the [DatabaseInitializer] for the Configurations database that creates
 * the schema if it does not currently exist.
 */
class ConfigsDatabaseInitializer(
// TODO inject via dependency injection framework
  private val databaseAvailablityCheck: ConfigsDatabaseAvailabilityCheck,
  // TODO inject via dependency injection framework
  private val dslContext: DSLContext,
  // TODO inject via dependency injection framework
  private val initialSchema: String,
) : DatabaseInitializer {
  override fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck? = databaseAvailablityCheck

  override fun getDatabaseName(): String = DatabaseConstants.CONFIGS_DATABASE_LOGGING_NAME

  override fun getDslContext(): DSLContext? = dslContext

  override fun getInitialSchema(): String = initialSchema

  override fun getLogger(): Logger = LOGGER

  override fun getTableNames(): Collection<String> = DatabaseConstants.CONFIGS_INITIAL_EXPECTED_TABLES

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(ConfigsDatabaseInitializer::class.java)
  }
}
