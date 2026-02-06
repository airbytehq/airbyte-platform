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
class V2_1_0_015__AddAgenticUserEnabledAtToUser : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addAgenticEnabledAtColumn(ctx)

    log.info { "Migration completed: ${javaClass.simpleName}" }
  }

  companion object {
    private const val USER_TABLE = "user"
    private const val AGENTIC_ENABLED_AT_COLUMN = "agentic_enabled_at"

    fun addAgenticEnabledAtColumn(ctx: DSLContext) {
      ctx
        .alterTable(USER_TABLE)
        .addColumnIfNotExists(
          DSL.field(AGENTIC_ENABLED_AT_COLUMN, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true)),
        ).execute()
    }
  }
}
