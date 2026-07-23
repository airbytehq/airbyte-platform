/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.GROUP_TABLE
import io.airbyte.db.instance.DatabaseConstants.PERMISSION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Add group_id foreign key to the Permission table to support group-based permissions.
 * This allows permissions to be assigned to groups in addition to individual users and service accounts.
 * The column is nullable to maintain backward compatibility with existing user and service account permissions.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_003__AddGroupIdToPermission : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    addGroupIdToPermission(ctx)

    log.info { "Migration finished!" }
  }

  companion object {
    fun addGroupIdToPermission(ctx: DSLContext) {
      val groupId = DSL.field("group_id", SQLDataType.UUID.nullable(true))

      // Add the group_id column to the permission table
      ctx
        .alterTable(PERMISSION_TABLE)
        .addColumn(groupId)
        .execute()

      // Add foreign key constraint
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(DSL.foreignKey<UUID>(groupId).references(GROUP_TABLE, "id").onDeleteCascade())
        .execute()

      // Create index for querying permissions by group
      ctx
        .createIndexIfNotExists("permission_group_id_idx")
        .on(PERMISSION_TABLE, groupId.name)
        .execute()
    }
  }
}
