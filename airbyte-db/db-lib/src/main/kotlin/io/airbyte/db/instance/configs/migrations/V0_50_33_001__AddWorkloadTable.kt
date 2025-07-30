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
class V0_50_33_001__AddWorkloadTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createEnumStatusType(ctx)
    createWorkload(ctx)
    createLabel(ctx)
    createWorkloadLabelIndex(ctx)
    log.info { "Migration finished!" }
  }

  internal enum class WorkloadStatus(
    private val literal: String,
  ) : EnumType {
    PENDING("pending"),
    CLAIMED("claimed"),
    RUNNING("running"),
    SUCCESS("success"),

    FAILURE("failure"),
    CANCELLED("cancelled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "workload_status"

    override fun getLiteral(): String = literal
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private const val WORKLOAD_TABLE_NAME = "workload"
    private const val WORKLOAD_ID_COLUMN_NAME = "id"
    private const val LABEL_TABLE_NAME = "workload_label"
    private const val LABEL_ID_COLUMN_NAME = "id"
    private const val WORKLOAD_STATUS = "workload_status"

    fun createWorkloadLabelIndex(ctx: DSLContext) {
      ctx
        .createIndexIfNotExists("workload_label_workload_id_idx")
        .on(LABEL_TABLE_NAME, "workload_id")
        .execute()
    }

    fun createWorkload(ctx: DSLContext) {
      val id = DSL.field(WORKLOAD_ID_COLUMN_NAME, SQLDataType.VARCHAR(256).nullable(false))
      // null when pending
      val dataplaneId = DSL.field("dataplane_id", SQLDataType.VARCHAR(256).nullable(true))
      val status = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(WorkloadStatus::class.java).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      // null while not heartbeating
      val lastHeartbeatAt = DSL.field("last_heartbeat_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))

      ctx
        .createTableIfNotExists(WORKLOAD_TABLE_NAME)
        .columns(
          id,
          dataplaneId,
          status,
          createdAt,
          updatedAt,
          lastHeartbeatAt,
        ).constraints(DSL.primaryKey(id))
        .execute()

      log.info { "workload table created" }
    }

    fun createLabel(ctx: DSLContext) {
      val id = DSL.field(LABEL_ID_COLUMN_NAME, SQLDataType.UUID.nullable(false))
      val workloadId = DSL.field("workload_id", SQLDataType.VARCHAR(256).nullable(false))
      val key = DSL.field("key", SQLDataType.VARCHAR(256).nullable(false))
      val value = DSL.field("value", SQLDataType.VARCHAR(256).nullable(false))

      ctx
        .createTableIfNotExists(LABEL_TABLE_NAME)
        .columns(
          id,
          workloadId,
          key,
          value,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(workloadId).references(WORKLOAD_TABLE_NAME, WORKLOAD_ID_COLUMN_NAME),
          DSL.unique(workloadId, key),
        ).execute()
      log.info { "workload label table created" }
    }

    private fun createEnumStatusType(ctx: DSLContext) {
      ctx
        .createType(WORKLOAD_STATUS)
        .asEnum(
          WorkloadStatus.CLAIMED.literal,
          WorkloadStatus.RUNNING.literal,
          WorkloadStatus.PENDING.literal,
          WorkloadStatus.SUCCESS.literal,
          WorkloadStatus.FAILURE.literal,
          WorkloadStatus.CANCELLED.literal,
        ).execute()
    }
  }
}
