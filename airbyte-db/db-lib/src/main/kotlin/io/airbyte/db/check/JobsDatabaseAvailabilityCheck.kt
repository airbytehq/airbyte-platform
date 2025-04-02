/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.airbyte.db.instance.DatabaseConstants
import org.jooq.DSLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Implementation of the [DatabaseAvailabilityCheck] for the Jobs database.
 */
class JobsDatabaseAvailabilityCheck(
// TODO inject via dependency injection framework
  private val dslContext: DSLContext, // TODO inject via dependency injection framework
  private val timeoutMs: Long,
) : DatabaseAvailabilityCheck {
  override fun getDatabaseName(): String = DatabaseConstants.JOBS_DATABASE_LOGGING_NAME

  override fun getDslContext(): DSLContext? = dslContext

  override fun getLogger(): Logger = LOGGER

  override fun getTimeoutMs(): Long = timeoutMs

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(JobsDatabaseAvailabilityCheck::class.java)
  }
}
