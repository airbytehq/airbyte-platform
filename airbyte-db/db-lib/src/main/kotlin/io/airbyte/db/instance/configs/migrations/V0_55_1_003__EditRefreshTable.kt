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
class V0_55_1_003__EditRefreshTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    editRefreshTable(ctx)
  }

  companion object {
    const val STREAM_REFRESHES_TABLE: String = "stream_refreshes"
    private val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    private val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false))
    private val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(true))

    private val createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    @JvmStatic
    fun editRefreshTable(ctx: DSLContext) {
      ctx.truncate(STREAM_REFRESHES_TABLE).execute()
      ctx.dropTable(STREAM_REFRESHES_TABLE).execute()

      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      ctx
        .createTable(STREAM_REFRESHES_TABLE)
        .columns(
          id,
          connectionId,
          streamName,
          streamNamespace,
          createdAtField,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
        ).execute()

      val indexCreationQuery = "CREATE INDEX ON $STREAM_REFRESHES_TABLE USING btree (${connectionId.name})"
      val indexCreationQuery2 = "CREATE INDEX ON $STREAM_REFRESHES_TABLE USING btree (${connectionId.name}, ${streamName.name})"
      val indexCreationQuery3 =
        "CREATE INDEX ON $STREAM_REFRESHES_TABLE USING btree (${connectionId.name}, ${streamName.name}, ${streamNamespace.name})"

      ctx.execute(indexCreationQuery)
      ctx.execute(indexCreationQuery2)
      ctx.execute(indexCreationQuery3)
    }
  }
}
