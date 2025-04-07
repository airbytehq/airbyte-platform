/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

import io.airbyte.db.check.DatabaseAvailabilityCheck
import io.airbyte.db.init.DatabaseInitializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext

private val log = KotlinLogging.logger {}

class ToysDatabaseInitializer(
  override val databaseAvailabilityCheck: DatabaseAvailabilityCheck,
  override val dslContext: DSLContext,
  override val initialSchema: String,
) : DatabaseInitializer {
  override val databaseName = ToysDatabaseConstants.DATABASE_LOGGING_NAME

  override val log = io.airbyte.db.instance.toys.log

  override val tableNames = setOf(ToysDatabaseConstants.TABLE_NAME)
}
