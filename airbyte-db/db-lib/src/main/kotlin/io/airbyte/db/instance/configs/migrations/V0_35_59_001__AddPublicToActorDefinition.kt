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
 * Add public column to actor definition migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_59_001__AddPublicToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addPublicColumn(ctx)
  }

  companion object {
    @JvmStatic
    fun addPublicColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("public", SQLDataType.BOOLEAN.nullable(false).defaultValue(false)))
        .execute()
    }
  }
}
