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
class V1_1_0_010__CreateTagTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createTagTable(ctx)
  }

  companion object {
    private const val TAG_TABLE = "tag"

    @JvmStatic
    fun createTagTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR.nullable(false))
      val color = DSL.field("color", SQLDataType.CHAR(6).nullable(false))
      val createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTable(TAG_TABLE)
        .columns(
          id,
          workspaceId,
          name,
          color,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.constraint("valid_hex_color").check(color.likeRegex("^[0-9A-Fa-f]{6}$")),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
          DSL.unique(name, workspaceId),
        ).execute()

      ctx
        .createIndexIfNotExists("tag_workspace_id_idx")
        .on("tag", workspaceId.name)
        .execute()
    }
  }
}
