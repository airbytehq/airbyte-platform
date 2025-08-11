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

@Suppress("ktlint:standard:class-naming")
class V1_6_0_021__CreateOrchestrationsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    createOrchestrationTable(ctx)
  }

  companion object {
    private const val ORCHESTRATION_TABLE = "orchestration"

    fun createOrchestrationTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val versionId = DSL.field("version_id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR.nullable(false))
      val orchestrationDefinition = DSL.field("orchestration_definition", SQLDataType.JSONB.nullable(false))
      val orchestrationSchedule = DSL.field("orchestration_schedule", SQLDataType.JSONB.nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.falseCondition()))

      ctx
        .createTableIfNotExists(ORCHESTRATION_TABLE)
        .columns(
          id,
          versionId,
          workspaceId,
          name,
          orchestrationDefinition,
          orchestrationSchedule,
          createdAt,
          updatedAt,
          tombstone,
        ).constraints(
          DSL.primaryKey(id, versionId),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("orchestration_workspace_id_idx")
        .on(ORCHESTRATION_TABLE, workspaceId.name)
        .execute()

      ctx
        .createIndexIfNotExists("orchestration_name_idx")
        .on(ORCHESTRATION_TABLE, name.name)
        .execute()
    }
  }
}
