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
class V0_50_33_011__AddScopedImpactToBreakingChangeTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addScopedImpactColumnToBreakingChangeTable(ctx)
  }

  companion object {
    private const val ACTOR_DEFINITION_BREAKING_CHANGE = "actor_definition_breaking_change"
    private const val SCOPED_IMPACT_COLUMN = "scoped_impact"

    fun addScopedImpactColumnToBreakingChangeTable(ctx: DSLContext) {
      ctx
        .alterTable(ACTOR_DEFINITION_BREAKING_CHANGE)
        .addColumnIfNotExists(DSL.field(SCOPED_IMPACT_COLUMN, SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
