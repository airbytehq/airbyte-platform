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

@Suppress("ktlint:standard:class-naming")
class V0_50_41_004__AddDeadlineColumnToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addDeadlineColumnToWorkload(ctx)
  }

  companion object {
    private const val WORKLOAD_TABLE = "workload"
    private const val DEADLINE_COLUMN = "deadline"

    @JvmStatic
    fun addDeadlineColumnToWorkload(ctx: DSLContext) {
      val createdAt =
        DSL.field(DEADLINE_COLUMN, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      ctx
        .alterTable("workload")
        .addColumnIfNotExists(createdAt)
        .execute()
      ctx.execute(
        String.format(
          "CREATE INDEX ON %s(%s) WHERE %s IS NOT NULL",
          WORKLOAD_TABLE,
          DEADLINE_COLUMN,
          DEADLINE_COLUMN,
        ),
      )
    }
  }
}
