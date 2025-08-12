/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_024__CreateOrchestrationTaskTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    dropOrchestrationDefinition(ctx)
    createOrchestrationTaskTypeEnumType(ctx)
    createOrchestrationTaskTable(ctx)
  }

  enum class OrchestrationTaskType(
    private val literal: String,
  ) : EnumType {
    SYNC("sync"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME = "orchestration_task_type"
    }
  }

  companion object {
    private const val ORCHESTRATION_TABLE = "orchestration"
    private const val ORCHESTRATION_TASK_TABLE = "orchestration_task"

    fun dropOrchestrationDefinition(ctx: DSLContext) {
      ctx
        .alterTable(ORCHESTRATION_TABLE)
        .dropColumnIfExists("orchestration_definition")
        .execute()
    }

    fun createOrchestrationTaskTypeEnumType(ctx: DSLContext) {
      ctx
        .createType(OrchestrationTaskType.NAME)
        .asEnum(*OrchestrationTaskType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createOrchestrationTaskTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val orchestrationId = DSL.field("orchestration_id", SQLDataType.UUID.nullable(false))
      val orchestrationVersionId = DSL.field("orchestration_version_id", SQLDataType.UUID.nullable(false))
      val taskType = DSL.field("type", SQLDataType.VARCHAR.nullable(false))
      val taskDefinitionId = DSL.field("task_definition_id", SQLDataType.UUID.nullable(false))
      val dependsOn = DSL.field("depends_on", SQLDataType.UUID.arrayDataType.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(ORCHESTRATION_TASK_TABLE)
        .columns(
          id,
          orchestrationId,
          orchestrationVersionId,
          taskType,
          taskDefinitionId,
          dependsOn,
          createdAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(orchestrationId, orchestrationVersionId).references("orchestration", "id", "version_id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("orchestration_task_orchestration_id_idx")
        .on(ORCHESTRATION_TASK_TABLE, orchestrationId.name)
        .execute()
    }
  }
}
