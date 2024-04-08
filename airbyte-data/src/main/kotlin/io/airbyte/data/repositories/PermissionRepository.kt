package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Permission
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
  fun findByIdIn(permissionIds: List<UUID>): List<Permission>

  fun findByUserId(userId: UUID): List<Permission>

  fun findByOrganizationId(organizationId: UUID): List<Permission>

  fun deleteByIdIn(permissionIds: List<UUID>)
}
