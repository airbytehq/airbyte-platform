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
class V1_6_0_025__CreateOrchestrationTaskRunTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    changeOrchestrationTaskColumnType(ctx)
    renameTaskDefinitionColumn(ctx)
    createTaskStatusEnumType(ctx)
    createOrchestrationRunTaskTableAndIndexes(ctx)
  }

  enum class OrchestrationTaskRunStatus(
    private val literal: String,
  ) : EnumType {
    PENDING("pending"),
    RUNNING("running"),
    INCOMPLETE("incomplete"),
    FAILED("failed"),
    SUCCEEDED("succeeded"),
    CANCELLED("cancelled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME = "orchestration_task_run_status"
    }
  }

  companion object {
    private const val ORCHESTRATION_TASK_RUN_TABLE = "orchestration_task_run"
    private const val ORCHESTRATION_TASK_TABLE = "orchestration_task"

    fun changeOrchestrationTaskColumnType(ctx: DSLContext) {
      ctx.execute("ALTER TABLE orchestration_task ALTER COLUMN \"type\" TYPE orchestration_task_type USING \"type\"::orchestration_task_type;")
    }

    fun renameTaskDefinitionColumn(ctx: DSLContext) {
      ctx
        .alterTable(ORCHESTRATION_TASK_TABLE)
        .renameColumn("task_definition_id")
        .to("task_scope_id")
        .execute()
    }

    fun createTaskStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(OrchestrationTaskRunStatus.NAME)
        .asEnum(*OrchestrationTaskRunStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createOrchestrationRunTaskTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val orchestrationRunId = DSL.field("orchestration_run_id", SQLDataType.UUID.nullable(false))
      val orchestrationTaskStatus =
        DSL.field(
          "orchestration_task_run_status",
          SQLDataType.VARCHAR.asEnumDataType(OrchestrationTaskRunStatus::class.java).nullable(false),
        )
      val taskId = DSL.field("task_id", SQLDataType.UUID.nullable(false))
      val taskRefId = DSL.field("task_ref_id", SQLDataType.VARCHAR.nullable(false))

      ctx
        .createTableIfNotExists(ORCHESTRATION_TASK_RUN_TABLE)
        .columns(
          id,
          orchestrationRunId,
          orchestrationTaskStatus,
          taskId,
          taskRefId,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(orchestrationRunId).references("orchestration_run", "id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("orchestration_task_run_orchestration_run_id_idx")
        .on(ORCHESTRATION_TASK_RUN_TABLE, orchestrationRunId.name)
        .execute()

      ctx
        .createIndexIfNotExists("orchestration_task_run_status_idx")
        .on(ORCHESTRATION_TASK_RUN_TABLE, orchestrationTaskStatus.name)
        .execute()

      ctx
        .createIndexIfNotExists("orchestration_task_run_task_id_idx")
        .on(ORCHESTRATION_TASK_RUN_TABLE, taskId.name)
        .execute()
    }
  }
}
