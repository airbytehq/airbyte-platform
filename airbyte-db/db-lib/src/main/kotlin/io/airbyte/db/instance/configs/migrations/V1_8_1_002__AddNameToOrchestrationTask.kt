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
class V1_8_1_002__AddNameToOrchestrationTask : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    addNameColumnToOrchestrationTask(ctx)
    changeDependsOnType(ctx)
    addUniquenessConstraint(ctx)
    dropOrchestrationIdIndex(ctx)
    renameColumns(ctx)
  }

  companion object {
    private const val ORCHESTRATION_TASK_TABLE = "orchestration_task"
    val taskName = DSL.field("name", SQLDataType.VARCHAR.nullable(false))

    fun addNameColumnToOrchestrationTask(ctx: DSLContext) {
      ctx
        .alterTable(ORCHESTRATION_TASK_TABLE)
        .addColumn(taskName)
        .execute()
    }

    fun changeDependsOnType(ctx: DSLContext) {
      ctx
        .execute("ALTER TABLE orchestration_task ALTER COLUMN depends_on TYPE VARCHAR[] USING depends_on::VARCHAR[]")
    }

    fun addUniquenessConstraint(ctx: DSLContext) {
      ctx
        .createUniqueIndex("name_unique_key")
        .on(ORCHESTRATION_TASK_TABLE, "orchestration_id", "orchestration_version_id", "name")
        .execute()
    }

    fun dropOrchestrationIdIndex(ctx: DSLContext) {
      ctx.dropIndex("orchestration_task_orchestration_id_idx").execute()
    }

    fun renameColumns(ctx: DSLContext) {
      ctx
        .alterTable(ORCHESTRATION_TASK_TABLE)
        .renameColumn("task_scope_id")
        .to("scope_id")
        .execute()

      ctx
        .alterTable("orchestration")
        .renameColumn("orchestration_schedule")
        .to("schedule")
        .execute()

      ctx
        .alterTable("orchestration_run")
        .renameColumn("orchestration_run_status")
        .to("status")
        .execute()

      ctx
        .alterTable("orchestration_task_run")
        .renameColumn("orchestration_task_run_status")
        .to("status")
        .execute()
    }
  }
}
