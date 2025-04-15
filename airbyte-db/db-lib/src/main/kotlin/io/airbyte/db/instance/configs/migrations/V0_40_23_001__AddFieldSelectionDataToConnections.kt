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
 * Add field selection to connection.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_23_001__AddFieldSelectionDataToConnections : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addFieldSelectionData(ctx)
  }

  companion object {
    private fun addFieldSelectionData(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(DSL.field("field_selection_data", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
