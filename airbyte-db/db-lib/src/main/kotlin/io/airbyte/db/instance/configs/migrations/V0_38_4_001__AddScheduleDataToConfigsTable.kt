/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add schedule to connection migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_38_4_001__AddScheduleDataToConfigsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addPublicColumn(ctx)
  }

  companion object {
    private fun addPublicColumn(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field(
            "schedule_data",
            SQLDataType.JSONB.nullable(true),
          ),
        ).execute()
    }
  }
}
