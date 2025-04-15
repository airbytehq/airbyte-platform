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

@Suppress("ktlint:standard:class-naming")
class V0_57_4_011__DropUserTableAuthColumns : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    dropAuthColumns(ctx)
  }

  companion object {
    fun dropAuthColumns(ctx: DSLContext) {
      ctx
        .alterTable("user")
        .dropColumnIfExists("auth_provider")
        .execute()

      ctx
        .alterTable("user")
        .dropColumnIfExists("auth_user_id")
        .execute()
    }
  }
}
