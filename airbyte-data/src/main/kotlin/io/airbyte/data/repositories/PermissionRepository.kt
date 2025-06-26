/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Permission
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

  fun deleteByIdIn(permissionIds: List<UUID>)

  @Query(
    """
    select * from permission p
    join auth_user au on p.user_id = au.user_id
    where au.auth_user_id = :authUserId
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
}

@Introspected
data class OrgMemberCount(
  val organizationId: UUID,
  val count: Int? = 0,
)
