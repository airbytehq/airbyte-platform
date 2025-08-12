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
class V1_6_0_022__CreateOrchestrationRunTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    createRunStatusTypeEnumType(ctx)
    createOrchestrationRunTableAndIndexes(ctx)
  }

  enum class OrchestrationRunStatus(
    private val literal: String,
  ) : EnumType {
    PENDING("pending"),
    RUNNING("running"),
    FAILED("failed"),
    SUCCEEDED("succeeded"),
    CANCELLED("cancelled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME = "orchestration_run_status"
    }
  }

  companion object {
    private const val ORCHESTRATION_RUN_TABLE = "orchestration_run"

    fun createRunStatusTypeEnumType(ctx: DSLContext) {
      ctx
        .createType(OrchestrationRunStatus.NAME)
        .asEnum(*OrchestrationRunStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createOrchestrationRunTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val orchestrationId = DSL.field("orchestration_id", SQLDataType.UUID.nullable(false))
      val orchestrationVersionId = DSL.field("orchestration_version_id", SQLDataType.UUID.nullable(false))
      val status = DSL.field("orchestration_run_status", SQLDataType.VARCHAR.asEnumDataType(OrchestrationRunStatus::class.java).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(ORCHESTRATION_RUN_TABLE)
        .columns(
          id,
          orchestrationId,
          orchestrationVersionId,
          status,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(orchestrationId, orchestrationVersionId).references("orchestration", "id", "version_id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("orchestration_run_orchestration_id_idx")
        .on(ORCHESTRATION_RUN_TABLE, orchestrationId.name)
        .execute()

      ctx
        .createIndexIfNotExists("orchestration_run_status_idx")
        .on(ORCHESTRATION_RUN_TABLE, status.name)
        .execute()
    }
  }
}
