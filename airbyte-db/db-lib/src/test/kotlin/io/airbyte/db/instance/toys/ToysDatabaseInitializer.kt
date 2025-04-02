/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.airbyte.db.init.DatabaseInitializer
import org.jooq.DSLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ToysDatabaseInitializer(
  private val databaseAvailablityCheck: DatabaseAvailabilityCheck,
  private val dslContext: DSLContext,
  private val initialSchema: String,
) : DatabaseInitializer {
  override fun getDatabaseAvailabilityCheck(): DatabaseAvailabilityCheck? = databaseAvailablityCheck

  override fun getDatabaseName(): String = ToysDatabaseConstants.DATABASE_LOGGING_NAME

  override fun getDslContext(): DSLContext? = dslContext

  override fun getInitialSchema(): String = initialSchema

  override fun getLogger(): Logger = LOGGER

  override fun getTableNames(): Collection<String> = setOf(ToysDatabaseConstants.TABLE_NAME)

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(ToysDatabaseInitializer::class.java)
  }
}
