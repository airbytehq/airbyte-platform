/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Permission
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

/**
 * Repository for managing permissions.
 * NOTE: eventually this will fully replace the PermissionPersistence class.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface PermissionRepository : PageableRepository<Permission, UUID> {
  fun find(): List<Permission>

  fun findByIdIn(permissionIds: List<UUID>): List<Permission>

  fun findByUserId(userId: UUID): List<Permission>

  fun findByServiceAccountId(serviceAccountId: UUID): List<Permission>

  fun findByOrganizationId(organizationId: UUID): List<Permission>

  fun findByWorkspaceId(workspaceId: UUID): List<Permission>

  fun findByGroupId(groupId: UUID): List<Permission>

  fun deleteByIdIn(permissionIds: List<UUID>)

  @Query(
    """
    DELETE FROM permission
    WHERE user_id = :userId
      AND organization_id = :organizationId
    """,
  )
  fun deleteByUserIdAndOrganizationId(
    userId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    DELETE FROM permission scoped_permission
    USING workspace scoped_workspace
    WHERE scoped_permission.user_id = :userId
      AND scoped_permission.workspace_id = scoped_workspace.id
      AND scoped_workspace.organization_id = :organizationId
    """,
  )
  fun deleteWorkspacePermissionsByUserIdAndOrganizationId(
    userId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    select direct_permission.*
    from permission direct_permission
    join auth_user au on direct_permission.user_id = au.user_id
    where au.auth_user_id = :authUserId
    and direct_permission.group_id is null
    and direct_permission.service_account_id is null
    union all
    select group_permission.*
    from auth_user au
    join group_member gm on gm.user_id = au.user_id
    join "group" g on g.id = gm.group_id
    join organization o on o.id = g.organization_id
    -- Keep this lookup correlated so PostgreSQL uses the user/organization index for high-membership users.
    join lateral (
      select 1
      from permission organization_membership
      where organization_membership.user_id = au.user_id
      and organization_membership.organization_id = o.id
      and organization_membership.workspace_id is null
      and organization_membership.group_id is null
      and organization_membership.service_account_id is null
      and organization_membership.permission_type in (
        'organization_admin',
        'organization_editor',
        'organization_runner',
        'organization_reader',
        'organization_member'
      )
      limit 1
    ) valid_organization_membership on true
    join permission group_permission on group_permission.group_id = g.id
    left join workspace w
      on w.id = group_permission.workspace_id
      and w.organization_id = o.id
    where au.auth_user_id = :authUserId
    and group_permission.user_id is null
    and group_permission.service_account_id is null
    and (
      (
        group_permission.organization_id = o.id
        and group_permission.workspace_id is null
        and group_permission.permission_type in (
          'organization_admin',
          'organization_editor',
          'organization_runner',
          'organization_reader',
          'organization_member'
        )
      )
      or (
        group_permission.organization_id is null
        and group_permission.workspace_id = w.id
        and group_permission.permission_type in (
          'workspace_admin',
          'workspace_editor',
          'workspace_runner',
          'workspace_reader'
        )
      )
    )
  """,
  )
  fun queryByAuthUser(authUserId: String): List<Permission>

  @Query(
    """
    select * from permission
    where exists (
      select * from "user"
      where lower("user".email) = lower(:email)
      and "user".id = permission.user_id
    )
  """,
  )
  fun findByUserEmail(email: String): List<Permission>

  @Query(
    """
      select organization_id as organization_id, count(user_id) as count
      from permission p
      join "user" u on p.user_id = u.id
      where p.organization_id in (:orgIds)
      group by p.organization_id
    """,
  )
  fun getMemberCountByOrgIdList(orgIds: List<UUID>): List<OrgMemberCount>

  @Query(
    """
    SELECT EXISTS (
      SELECT 1 FROM permission
      WHERE user_id = :userId
      AND permission_type = 'instance_admin'
    )
    """,
  )
  fun isInstanceAdmin(userId: UUID): Boolean

  @Query(
    """
    SELECT EXISTS (
      SELECT 1 FROM permission
      WHERE user_id = :userId
      AND organization_id = :organizationId
    )
    """,
  )
  fun existsByUserIdAndOrganizationId(
    userId: UUID,
    organizationId: UUID,
  ): Boolean

  @Query(
    """
    SELECT EXISTS (
      SELECT 1 FROM permission
      WHERE group_id = :groupId
      AND permission_type = :permissionType
      AND organization_id = :organizationId
    )
    """,
  )
  fun existsByGroupIdAndPermissionTypeAndOrganizationId(
    groupId: UUID,
    permissionType: PermissionType,
    organizationId: UUID,
  ): Boolean

  @Query(
    """
    SELECT EXISTS (
      SELECT 1 FROM permission
      WHERE group_id = :groupId
      AND permission_type = :permissionType
      AND workspace_id = :workspaceId
    )
    """,
  )
  fun existsByGroupIdAndPermissionTypeAndWorkspaceId(
    groupId: UUID,
    permissionType: PermissionType,
    workspaceId: UUID,
  ): Boolean
}

@Introspected
data class OrgMemberCount(
  val organizationId: UUID,
  val count: Int? = 0,
)
