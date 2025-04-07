/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.airbyte.db.instance.DatabaseConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext

private val log = KotlinLogging.logger { }

/**
 * Implementation of the [DatabaseAvailabilityCheck] for the Configurations database.
 *
 * TODO inject via dependency injection framework
 */
class ConfigsDatabaseAvailabilityCheck(
  override val dslContext: DSLContext,
  override val timeoutMs: Long,
) : DatabaseAvailabilityCheck {
  override val databaseName = DatabaseConstants.CONFIGS_DATABASE_LOGGING_NAME

  override val log = io.airbyte.db.check.log
}
