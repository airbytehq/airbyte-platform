/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drop configs table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_3_001__DropAirbyteConfigsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    dropTable(ctx)
  }

  companion object {
    @JvmStatic
    fun dropTable(ctx: DSLContext) {
      ctx.dropTableIfExists(DSL.table("airbyte_configs")).execute()
    }
  }
}
