/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.time.OffsetDateTime

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_025__CreateOAuthStateTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createOAuthStateTable(ctx)
  }

  companion object {
    val id: Field<String> = DSL.field("id", SQLDataType.VARCHAR.notNull())
    val state: Field<String> = DSL.field("state", SQLDataType.VARCHAR.notNull())

    // row timestamps
    val createdAt: Field<OffsetDateTime> =
      DSL.field(
        "created_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()),
      )
    val updatedAt: Field<OffsetDateTime> =
      DSL.field(
        "updated_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()),
      )

    private fun createOAuthStateTable(ctx: DSLContext) {
      ctx
        .createTableIfNotExists("oauth_state")
        .columns(id, state, createdAt, updatedAt)
        .constraints(DSL.primaryKey(id))
        .execute()
    }
  }
}
