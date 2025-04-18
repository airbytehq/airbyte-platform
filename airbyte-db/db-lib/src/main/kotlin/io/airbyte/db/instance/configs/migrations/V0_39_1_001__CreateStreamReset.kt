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
 * Create stream reset table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_39_1_001__CreateStreamReset : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createStreamResetTable(ctx)
  }

  companion object {
    private fun createStreamResetTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val streamNamespace = DSL.field("stream_namespace", SQLDataType.CLOB.nullable(true))
      val streamName = DSL.field("stream_name", SQLDataType.CLOB.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("stream_reset")
        .columns(id, connectionId, streamNamespace, streamName, createdAt, updatedAt)
        .constraints(
          DSL.unique(connectionId, streamName, streamNamespace),
        ).execute()

      ctx
        .createIndex("connection_id_stream_name_namespace_idx")
        .on("stream_reset", "connection_id", "stream_name", "stream_namespace")
        .execute()
    }
  }
}
