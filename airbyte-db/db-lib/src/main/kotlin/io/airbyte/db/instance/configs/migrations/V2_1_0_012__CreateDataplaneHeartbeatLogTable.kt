/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_012__CreateDataplaneHeartbeatLogTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  companion object {
    private const val DATAPLANE_HEARTBEAT_LOG_TABLE = "dataplane_heartbeat_log"
    private const val DATAPLANE_TABLE = "dataplane"
    private const val ID_FIELD_NAME = "id"
    private const val DATAPLANE_ID_FIELD_NAME = "dataplane_id"
    private const val CONTROL_PLANE_VERSION_FIELD_NAME = "control_plane_version"
    private const val DATAPLANE_VERSION_FIELD_NAME = "dataplane_version"
    private const val CREATED_AT_FIELD_NAME = "created_at"

    fun migrate(ctx: DSLContext) {
      createDataplaneHeartbeatLogTable(ctx)
      createQueryIndex(ctx)
      createCleanupIndex(ctx)
    }

    private fun createDataplaneHeartbeatLogTable(ctx: DSLContext) {
      val id =
        DSL.field(
          ID_FIELD_NAME,
          org.jooq.impl.SQLDataType.UUID
            .nullable(false),
        )
      val dataplaneId =
        DSL.field(
          DATAPLANE_ID_FIELD_NAME,
          org.jooq.impl.SQLDataType.UUID
            .nullable(false),
        )
      val controlPlaneVersion =
        DSL.field(
          CONTROL_PLANE_VERSION_FIELD_NAME,
          org.jooq.impl.SQLDataType
            .VARCHAR(50)
            .nullable(false),
        )
      val dataplaneVersion =
        DSL.field(
          DATAPLANE_VERSION_FIELD_NAME,
          org.jooq.impl.SQLDataType
            .VARCHAR(50)
            .nullable(false),
        )
      val createdAt =
        DSL.field(
          CREATED_AT_FIELD_NAME,
          org.jooq.impl.SQLDataType.TIMESTAMPWITHTIMEZONE
            .nullable(false)
            .defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTable(DATAPLANE_HEARTBEAT_LOG_TABLE)
        .columns(id, dataplaneId, controlPlaneVersion, dataplaneVersion, createdAt)
        .constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(dataplaneId).references(DATAPLANE_TABLE, ID_FIELD_NAME).onDeleteCascade(),
        ).execute()
    }

    private fun createQueryIndex(ctx: DSLContext) {
      // Composite index for health status and history queries
      // created_at DESC matches our DISTINCT ON query pattern for optimal performance
      ctx
        .createIndex("idx_dataplane_heartbeat_log_dataplane_created_at")
        .on(
          DSL.table(DATAPLANE_HEARTBEAT_LOG_TABLE),
          DSL.field(DATAPLANE_ID_FIELD_NAME),
          DSL.field(CREATED_AT_FIELD_NAME).desc(),
        ).execute()
    }

    private fun createCleanupIndex(ctx: DSLContext) {
      // Index for efficient deletion of old records by timestamp
      ctx
        .createIndex("idx_dataplane_heartbeat_log_created_at")
        .on(DATAPLANE_HEARTBEAT_LOG_TABLE, CREATED_AT_FIELD_NAME)
        .execute()
    }
  }
}
