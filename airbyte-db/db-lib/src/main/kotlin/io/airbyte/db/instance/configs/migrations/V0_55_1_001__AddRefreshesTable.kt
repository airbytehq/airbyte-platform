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
class V0_55_1_001__AddRefreshesTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createRefreshTable(ctx)
  }

  companion object {
    private const val STREAM_REFRESHES_TABLE = "stream_refreshes"

    private val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    private val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false))
    private val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(true))

    private val createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    fun createRefreshTable(ctx: DSLContext) {
      ctx
        .createTable(STREAM_REFRESHES_TABLE)
        .columns(
          connectionId,
          streamName,
          streamNamespace,
          createdAtField,
        ).constraints(
          DSL.primaryKey(connectionId, streamName, streamNamespace),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
        ).execute()
      ctx
        .createIndexIfNotExists("stream_refreshes_connection_id_idx")
        .on(STREAM_REFRESHES_TABLE, connectionId.name)
        .execute()
    }
  }
}
