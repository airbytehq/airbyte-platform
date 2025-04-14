/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_0_002__BreakingChangesDeadlineAction : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.alterTable(BREAKING_CHANGE_TABLE).add(DEADLINE_ACTION).execute()
  }

  companion object {
    private val BREAKING_CHANGE_TABLE = DSL.table("actor_definition_breaking_change")
    private val DEADLINE_ACTION = DSL.field("deadline_action", SQLDataType.VARCHAR(256))
  }
}
