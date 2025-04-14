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

/**
 * Add icon_url to actor_definition table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_33_016__AddIconUrlToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addIconUrlToActorDefinition(ctx)
  }

  companion object {
    @JvmStatic
    fun addIconUrlToActorDefinition(ctx: DSLContext) {
      val iconUrlColumn = DSL.field("icon_url", SQLDataType.VARCHAR(256).nullable(true))

      ctx
        .alterTable("actor_definition")
        .addIfNotExists(iconUrlColumn)
        .execute()
    }
  }
}
