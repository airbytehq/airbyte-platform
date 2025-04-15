/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_41_008__AddConnectionTimeline : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx
      .createTable(TABLE_NAME)
      .columns(
        idField,
        connectionIdField,
        userIdField,
        eventTypeField,
        summaryField,
        createdAtField,
      ).constraints(
        DSL.primaryKey(idField),
        DSL.foreignKey(connectionIdField).references("connection", "id").onDeleteCascade(),
        DSL.foreignKey(userIdField).references("user", "id"),
      ).execute()
    ctx
      .createIndexIfNotExists("idx_connection_timeline_connection_id")
      .on(DSL.table(TABLE_NAME), connectionIdField.asc(), createdAtField.desc(), eventTypeField.asc())
      .execute()
  }

  companion object {
    private val idField = DSL.field("id", SQLDataType.UUID.nullable(false))
    private val connectionIdField = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    private val userIdField = DSL.field("user_id", SQLDataType.UUID.nullable(true))
    private val eventTypeField = DSL.field("event_type", SQLDataType.VARCHAR.nullable(false))
    private val summaryField = DSL.field("summary", SQLDataType.JSONB.nullable(true))
    private val createdAtField = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    const val TABLE_NAME: String = "connection_timeline_event"
  }
}
