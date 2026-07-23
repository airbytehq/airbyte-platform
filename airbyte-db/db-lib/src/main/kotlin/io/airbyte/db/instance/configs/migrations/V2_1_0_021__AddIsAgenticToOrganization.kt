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
class V2_1_0_021__AddIsAgenticToOrganization : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addIsAgenticColumn(ctx)

    log.info { "Migration completed: ${javaClass.simpleName}" }
  }

  companion object {
    private const val ORGANIZATION_TABLE = "organization"
    private const val IS_AGENTIC_COLUMN = "is_agentic"

    fun addIsAgenticColumn(ctx: DSLContext) {
      ctx
        .alterTable(ORGANIZATION_TABLE)
        .addColumnIfNotExists(
          DSL.field(IS_AGENTIC_COLUMN, SQLDataType.BOOLEAN.nullable(false).defaultValue(false)),
        ).execute()
    }
  }
}
