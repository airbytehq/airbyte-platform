/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Group
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

/**
 * Repository for managing Group entities.
 * Handles write operations (save, update, delete) and simple queries.
 * For read operations that need computed fields like member counts,
 * use GroupWithMemberCountRepository instead.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupRepository : PageableRepository<Group, UUID> {
  /**
   * Find a group by name within a specific organization.
   *
   * @param name The group name
   * @param organizationId The organization ID
   * @return Group if found, null otherwise
   */
  fun findByNameAndOrganizationId(
    name: String,
    organizationId: UUID,
  ): Group?

  /**
   * Check if a group name already exists within an organization.
   *
   * @param name The group name
   * @param organizationId The organization ID
   * @return true if a group with this name exists in the organization
   */
  fun existsByNameAndOrganizationId(
    name: String,
    organizationId: UUID,
  ): Boolean

  /**
   * Get the count of groups in an organization.
   *
   * @param organizationId The organization ID
   * @return Number of groups in the organization
   */
  fun countByOrganizationId(organizationId: UUID): Long
}
