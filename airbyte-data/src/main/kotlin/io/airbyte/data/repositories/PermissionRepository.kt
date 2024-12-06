package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Permission
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

  fun findByOrganizationId(organizationId: UUID): List<Permission>

  fun deleteByIdIn(permissionIds: List<UUID>)

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
}
