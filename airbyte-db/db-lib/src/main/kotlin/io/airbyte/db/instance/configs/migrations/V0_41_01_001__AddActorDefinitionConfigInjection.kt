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
 * Add config injection table to the platform.
 */
@Suppress("ktlint:standard:class-naming")
class V0_41_01_001__AddActorDefinitionConfigInjection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addConfigInjectionTable(ctx)
  }

  companion object {
    private fun addConfigInjectionTable(ctx: DSLContext) {
      val jsonToInject = DSL.field("json_to_inject", SQLDataType.JSONB.nullable(false))
      val injectionPath = DSL.field("injection_path", SQLDataType.VARCHAR.nullable(false))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("actor_definition_config_injection")
        .columns(jsonToInject, injectionPath, actorDefinitionId, createdAt, updatedAt)
        .constraints(
          DSL.primaryKey(actorDefinitionId, injectionPath),
          DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade(),
          DSL.unique(actorDefinitionId, injectionPath),
        ).execute()
    }
  }
}
