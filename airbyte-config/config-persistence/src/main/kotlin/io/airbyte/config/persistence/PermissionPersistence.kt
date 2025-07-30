/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.enums.toEnum
import io.airbyte.config.Permission
import io.airbyte.config.User
import io.airbyte.config.UserPermission
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Record6
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors

/**
 * Permission Persistence.
 *
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 *
 */
@Deprecated("to be replaced by {@link io.airbyte.data.repositories.PermissionRepository}")
class PermissionPersistence(
  database: Database?,
) {
  private val database = ExceptionWrappingDatabase(database)

  @Throws(IOException::class)
  fun listInstanceAdminUsers(): List<UserPermission> =
    database.query { ctx: DSLContext ->
      this.listInstanceAdminPermissions(
        ctx,
      )
    }

  @Throws(IOException::class)
  fun listUsersInOrganization(organizationId: UUID): List<UserPermission> =
    database.query { ctx: DSLContext ->
      listPermissionsForOrganization(
        ctx,
        organizationId,
      )
    }

  private fun listInstanceAdminPermissions(ctx: DSLContext): List<UserPermission> {
    val records =
      ctx
        .select(
          Tables.USER.ID,
          Tables.USER.NAME,
          Tables.USER.EMAIL,
          Tables.USER.DEFAULT_WORKSPACE_ID,
          Tables.PERMISSION.ID,
          Tables.PERMISSION.PERMISSION_TYPE,
        ).from(Tables.PERMISSION)
        .join(Tables.USER)
        .on(Tables.PERMISSION.USER_ID.eq(Tables.USER.ID))
        .where(Tables.PERMISSION.PERMISSION_TYPE.eq(PermissionType.instance_admin))
        .fetch()

    return records
      .stream()
      .map { record: Record6<UUID, String, String, UUID, UUID, PermissionType> -> this.buildUserPermissionFromRecord(record) }
      .collect(
        Collectors.toList(),
      )
  }

  @Throws(IOException::class)
  fun findPermissionTypeForUserAndWorkspace(
    workspaceId: UUID,
    authUserId: String,
  ): Permission.PermissionType? =
    database.query { ctx: DSLContext ->
      findPermissionTypeForUserAndWorkspace(
        ctx,
        workspaceId,
        authUserId,
      )
    }

  private fun findPermissionTypeForUserAndWorkspace(
    ctx: DSLContext,
    workspaceId: UUID,
    authUserId: String,
  ): Permission.PermissionType? {
    val record =
      ctx
        .select(Tables.PERMISSION.PERMISSION_TYPE)
        .from(Tables.PERMISSION)
        .join(Tables.USER)
        .on(Tables.PERMISSION.USER_ID.eq(Tables.USER.ID))
        .join(Tables.AUTH_USER)
        .on(Tables.USER.ID.eq(Tables.AUTH_USER.USER_ID))
        .where(Tables.PERMISSION.WORKSPACE_ID.eq(workspaceId))
        .and(Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne()
    if (record == null) {
      return null
    }

    val jooqPermissionType =
      record.get(
        Tables.PERMISSION.PERMISSION_TYPE,
        PermissionType::class.java,
      )

    return jooqPermissionType.literal.toEnum<Permission.PermissionType>()!!
  }

  @Throws(IOException::class)
  fun findPermissionTypeForUserAndOrganization(
    organizationId: UUID,
    authUserId: String,
  ): Permission.PermissionType? =
    database.query { ctx: DSLContext ->
      findPermissionTypeForUserAndOrganization(
        ctx,
        organizationId,
        authUserId,
      )
    }

  private fun findPermissionTypeForUserAndOrganization(
    ctx: DSLContext,
    organizationId: UUID,
    authUserId: String,
  ): Permission.PermissionType? {
    val record =
      ctx
        .select(Tables.PERMISSION.PERMISSION_TYPE)
        .from(Tables.PERMISSION)
        .join(Tables.USER)
        .on(Tables.PERMISSION.USER_ID.eq(Tables.USER.ID))
        .join(Tables.AUTH_USER)
        .on(Tables.USER.ID.eq(Tables.AUTH_USER.USER_ID))
        .where(Tables.PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .and(Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
        .fetchOne()

    if (record == null) {
      return null
    }

    val jooqPermissionType =
      record.get(
        Tables.PERMISSION.PERMISSION_TYPE,
        PermissionType::class.java,
      )
    return jooqPermissionType.literal.toEnum<Permission.PermissionType>()!!
  }

  /**
   * List all organization-level permissions for an organization.
   */
  @Throws(IOException::class)
  fun listPermissionsForOrganization(organizationId: UUID): List<UserPermission> =
    database.query { ctx: DSLContext ->
      listPermissionsForOrganization(
        ctx,
        organizationId,
      )
    }

  private fun listPermissionsForOrganization(
    ctx: DSLContext,
    organizationId: UUID,
  ): List<UserPermission> {
    val records =
      ctx
        .select(
          Tables.USER.ID,
          Tables.USER.NAME,
          Tables.USER.EMAIL,
          Tables.USER.DEFAULT_WORKSPACE_ID,
          Tables.PERMISSION.ID,
          Tables.PERMISSION.PERMISSION_TYPE,
        ).from(Tables.PERMISSION)
        .join(Tables.USER)
        .on(Tables.PERMISSION.USER_ID.eq(Tables.USER.ID))
        .where(Tables.PERMISSION.ORGANIZATION_ID.eq(organizationId))
        .fetch()

    return records
      .stream()
      .map { record: Record6<UUID, String, String, UUID, UUID, PermissionType> -> this.buildUserPermissionFromRecord(record) }
      .collect(
        Collectors.toList(),
      )
  }

  private fun buildUserPermissionFromRecord(record: Record): UserPermission =
    UserPermission()
      .withUser(
        User()
          .withUserId(record.get(Tables.USER.ID))
          .withName(record.get(Tables.USER.NAME))
          .withEmail(record.get(Tables.USER.EMAIL))
          .withDefaultWorkspaceId(record.get(Tables.USER.DEFAULT_WORKSPACE_ID)),
      ).withPermission(
        Permission()
          .withPermissionId(record.get(Tables.PERMISSION.ID))
          .withPermissionType(
            record.get(Tables.PERMISSION.PERMISSION_TYPE).toString().toEnum<Permission.PermissionType>()!!,
          ),
      )
}
