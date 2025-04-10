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
 * Add Connector Builder tables to the platform.
 */
@Suppress("ktlint:standard:class-naming")
class V0_41_00_001__AddConnectorBuilderProjectTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addConnectorBuilderProjectTable(ctx)
  }

  companion object {
    private fun addConnectorBuilderProjectTable(ctx: DSLContext) {
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val manifestDraft = DSL.field("manifest_draft", SQLDataType.JSONB.nullable(true))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(true))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("connector_builder_project")
        .columns(id, workspaceId, name, manifestDraft, actorDefinitionId, tombstone, createdAt, updatedAt)
        .constraints(DSL.primaryKey(id))
        .execute()
      ctx
        .createIndexIfNotExists("connector_builder_project_workspace_idx")
        .on("connector_builder_project", "workspace_id")
        .execute()
    }
  }
}
