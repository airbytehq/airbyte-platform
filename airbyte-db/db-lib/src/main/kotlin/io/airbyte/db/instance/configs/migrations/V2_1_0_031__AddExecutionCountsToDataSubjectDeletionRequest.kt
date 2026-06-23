/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.DATA_SUBJECT_DELETION_REQUEST_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Adds final DSR execution counts captured after the destructive execute phase finishes.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_031__AddExecutionCountsToDataSubjectDeletionRequest : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addExecutionCountsColumn(ctx)
  }

  companion object Companion {
    fun addExecutionCountsColumn(ctx: DSLContext) {
      ctx.execute("ALTER TABLE $DATA_SUBJECT_DELETION_REQUEST_TABLE ADD COLUMN execution_counts jsonb")
    }
  }
}
