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
 * Inserts a support_level column to the actor_definition_version table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_57_4_007__AddInternalSupportLevelToActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addInternalSupportLevelToActorDefinitionVersion(ctx)
    log.info { "internal_support_level column added to actor_definition_version table" }
  }

  companion object {
    fun addInternalSupportLevelToActorDefinitionVersion(ctx: DSLContext) {
      addInternalSupportLevelColumn(ctx)
    }

    fun addInternalSupportLevelColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("internal_support_level", SQLDataType.BIGINT.nullable(false).defaultValue(100L)))
        .execute()
    }
  }
}
