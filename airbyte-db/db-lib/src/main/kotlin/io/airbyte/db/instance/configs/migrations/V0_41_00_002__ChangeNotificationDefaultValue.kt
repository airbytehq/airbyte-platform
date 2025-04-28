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
 * Drop the notification column and re-add it with a new default value.
 */
@Suppress("ktlint:standard:class-naming")
class V0_41_00_002__ChangeNotificationDefaultValue : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    dropNotifySchemaChanges(ctx)
    addNotifySchemaChanges(ctx)
  }

  companion object {
    private fun dropNotifySchemaChanges(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .dropColumn(DSL.field("notify_schema_changes"))
        .execute()
    }

    private fun addNotifySchemaChanges(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(DSL.field("notify_schema_changes", SQLDataType.BOOLEAN.nullable(false).defaultValue(false)))
        .execute()
    }
  }
}
