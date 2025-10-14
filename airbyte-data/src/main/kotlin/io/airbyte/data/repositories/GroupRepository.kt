/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Group
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.Page
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

/**
 * Repository for managing Group entities.
 * Provides CRUD operations and custom queries for group management.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupRepository : PageableRepository<Group, UUID> {
  /**
   * Find a group by name within a specific organization.
   *
   * @param name The group name
   * @param organizationId The organization ID
   * @return Optional containing the group if found
   */
  fun findByNameAndOrganizationId(
    name: String,
    organizationId: UUID,
  ): Group?

  /**
   * Find all groups for a specific organization.
   *
   * @param organizationId The organization ID
   * @return List of groups in the organization
   */
  fun findByOrganizationId(organizationId: UUID): List<Group>

  /**
   * Find all groups for a specific organization with pagination.
   *
   * @param organizationId The organization ID
   * @param pageable Pagination parameters
   * @return Page of groups in the organization
   */
  fun findByOrganizationId(
    organizationId: UUID,
    pageable: Pageable,
  ): Page<Group>

  /**
   * Find groups by their IDs.
   *
   * @param ids Set of group IDs
   * @return List of groups matching the IDs
   */
  fun findByIdIn(ids: Set<UUID>): List<Group>

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

  /**
   * Find all groups that a user belongs to.
   *
   * @param userId The user ID
   * @return List of groups the user is a member of
   */
  @Query(
    """
    SELECT g.* FROM "group" g
    INNER JOIN group_member gm ON g.id = gm.group_id
    WHERE gm.user_id = :userId
    """,
  )
  fun findGroupsByUserId(userId: UUID): List<Group>
}
