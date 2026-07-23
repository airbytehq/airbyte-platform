/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_019__DropUnusedTemplateTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropTables(ctx)
  }

  companion object {
    fun dropTables(ctx: DSLContext) {
      // partial_user_config has a FK to config_template, so drop it first
      ctx.dropTableIfExists("partial_user_config").cascade().execute()
      ctx.dropTableIfExists("config_template").cascade().execute()
      ctx.dropTableIfExists("connection_template").cascade().execute()
    }
  }
}
