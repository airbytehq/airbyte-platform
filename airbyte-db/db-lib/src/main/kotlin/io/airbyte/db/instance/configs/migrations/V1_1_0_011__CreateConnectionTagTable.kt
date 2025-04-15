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
class V1_1_0_011__CreateConnectionTagTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  companion object {
    private const val CONNECTION_TAG_TABLE = "connection_tag"

    @JvmStatic
    fun migrate(ctx: DSLContext) {
      addPrimaryKeyToTagTable(ctx)
      createConnectionTagTable(ctx)
    }

    private fun addPrimaryKeyToTagTable(ctx: DSLContext) {
      ctx
        .alterTable("tag")
        .add(DSL.primaryKey("id"))
        .execute()
    }

    private fun createConnectionTagTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val tagId = DSL.field("tag_id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTable(CONNECTION_TAG_TABLE)
        .columns(
          id,
          tagId,
          connectionId,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(tagId).references("tag", "id").onDeleteCascade(),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
          DSL.unique(tagId, connectionId),
        ).execute()
    }
  }
}
