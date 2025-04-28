/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add failure summary migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_5_001__Add_failureSummary_col_to_Attempts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    @JvmStatic
    fun migrate(ctx: DSLContext) {
      addFailureSummaryColumn(ctx)
    }

    @JvmStatic
    fun addFailureSummaryColumn(ctx: DSLContext) {
      ctx
        .alterTable("attempts")
        .addColumnIfNotExists(DSL.field("failure_summary", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
