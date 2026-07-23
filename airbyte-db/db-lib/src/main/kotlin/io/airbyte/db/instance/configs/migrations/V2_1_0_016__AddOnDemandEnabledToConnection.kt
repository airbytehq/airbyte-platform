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

/**
 * Add on_demand_enabled column to connection table for on-demand capacity feature.
 *
 * When enabled, a connection will use on-demand capacity if committed capacity is exhausted,
 * allowing it to always run at scheduled time or when manually triggered.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_016__AddOnDemandEnabledToConnection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addOnDemandEnabledToConnection(ctx)
  }

  private fun addOnDemandEnabledToConnection(ctx: DSLContext) {
    ctx
      .alterTable("connection")
      .addColumn(
        DSL.field("on_demand_enabled", SQLDataType.BOOLEAN.nullable(false).defaultValue(false)),
      ).execute()
  }
}
