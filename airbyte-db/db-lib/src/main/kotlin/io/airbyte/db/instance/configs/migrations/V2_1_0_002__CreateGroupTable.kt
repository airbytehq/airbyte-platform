/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.GROUP_MEMBER_TABLE
import io.airbyte.db.instance.DatabaseConstants.GROUP_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Create the Group and GroupMember tables to support user groups functionality.
 * Groups are organization-scoped collections of users that can have permissions assigned to them.
 * GroupMember is a join table establishing many-to-many relationships between users and groups.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_002__CreateGroupTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    createGroupTable(ctx)
    createGroupMemberTable(ctx)

    log.info { "Migration finished!" }
  }

  companion object {
    fun createGroupTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val description = DSL.field("description", SQLDataType.VARCHAR(1024).nullable(true))
      val organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists(GROUP_TABLE)
        .columns(
          id,
          name,
          description,
          organizationId,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.unique(organizationId, name),
          DSL.foreignKey<UUID>(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade(),
        ).execute()

      // Index for querying groups by name within an organization
      ctx
        .createIndexIfNotExists("group_organization_id_name_idx")
        .on(GROUP_TABLE, organizationId.name, name.name)
        .execute()
    }

    fun createGroupMemberTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val groupId = DSL.field("group_id", SQLDataType.UUID.nullable(false))
      val userId = DSL.field("user_id", SQLDataType.UUID.nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists(GROUP_MEMBER_TABLE)
        .columns(
          id,
          groupId,
          userId,
          createdAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.unique(groupId, userId),
          DSL.foreignKey<UUID>(groupId).references(GROUP_TABLE, "id").onDeleteCascade(),
          DSL.foreignKey<UUID>(userId).references(USER_TABLE, "id").onDeleteCascade(),
        ).execute()

      // Index for querying groups by user
      ctx
        .createIndexIfNotExists("group_member_user_id_idx")
        .on(GROUP_MEMBER_TABLE, userId.name)
        .execute()

      // Composite index for efficient lookups of specific user-group relationships
      ctx
        .createIndexIfNotExists("group_member_group_id_user_id_idx")
        .on(GROUP_MEMBER_TABLE, groupId.name, userId.name)
        .execute()
    }
  }
}
