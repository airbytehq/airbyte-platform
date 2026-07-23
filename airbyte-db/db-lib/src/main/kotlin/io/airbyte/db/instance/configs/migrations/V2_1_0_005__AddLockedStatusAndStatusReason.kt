/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_005__AddLockedStatusAndStatusReason : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    addLockedValueToStatusType(ctx)
    addStatusReasonColumn(ctx)
  }

  companion object {
    fun addLockedValueToStatusType(ctx: DSLContext) {
      ctx.execute("ALTER TYPE status_type ADD VALUE 'locked'")
    }

    fun addStatusReasonColumn(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumn("status_reason", SQLDataType.VARCHAR(128).nullable(true))
        .execute()
    }
  }
}
