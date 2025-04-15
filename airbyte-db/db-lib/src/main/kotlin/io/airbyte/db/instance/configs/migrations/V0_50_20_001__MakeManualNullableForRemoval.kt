/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * A migration to make the "manual" column on the connection table nullable.
 *
 * This is so we can stop writing to it, and then remove it altogether.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_20_001__MakeManualNullableForRemoval : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    makeManualNullable(ctx)
  }

  companion object {
    private val MANUAL_COLUMN = DSL.field("manual", SQLDataType.BOOLEAN)

    private fun makeManualNullable(context: DSLContext) {
      context
        .alterTable(DSL.table(CONNECTION_TABLE))
        .alter(MANUAL_COLUMN)
        .dropNotNull()
        .execute()
    }
  }
}
