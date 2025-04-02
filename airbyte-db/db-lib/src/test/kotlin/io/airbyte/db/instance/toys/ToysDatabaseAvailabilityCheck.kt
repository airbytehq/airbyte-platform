/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

import io.airbyte.db.check.DatabaseAvailabilityCheck
import org.jooq.DSLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ToysDatabaseAvailabilityCheck(
  private val dslContext: DSLContext,
  private val timeoutMs: Long,
) : DatabaseAvailabilityCheck {
  override fun getDatabaseName(): String = ToysDatabaseConstants.DATABASE_LOGGING_NAME

  override fun getDslContext(): DSLContext? = dslContext

  override fun getLogger(): Logger = LOGGER

  override fun getTimeoutMs(): Long = timeoutMs

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(ToysDatabaseAvailabilityCheck::class.java)
  }
}
