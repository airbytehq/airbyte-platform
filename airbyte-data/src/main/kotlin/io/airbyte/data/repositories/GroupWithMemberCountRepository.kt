/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.GroupWithMemberCount
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.Optional
import java.util.UUID

/**
 * Read-only repository for querying Groups with computed member counts.
 * All queries include a JOIN with group_member table to populate the memberCount field.
 * Use GroupRepository for write operations (save, update, delete).
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupWithMemberCountRepository : GenericRepository<GroupWithMemberCount, UUID> {
  /**
   * Find a group by ID with member count.
   *
   * @param id The group ID
   * @return Optional containing the group with member count if found
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm.user_id), 0) as member_count
    FROM "group" g
    LEFT JOIN group_member gm ON g.id = gm.group_id
    WHERE g.id = :id
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    """,
    nativeQuery = true,
  )
  fun findById(id: UUID): Optional<GroupWithMemberCount>

  /**
   * Find a group by name within a specific organization with member count.
   *
   * @param name The group name
   * @param organizationId The organization ID
   * @return Group with member count if found, null otherwise
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm.user_id), 0) as member_count
    FROM "group" g
    LEFT JOIN group_member gm ON g.id = gm.group_id
    WHERE g.name = :name AND g.organization_id = :organizationId
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    """,
    nativeQuery = true,
  )
  fun findByNameAndOrganizationId(
    name: String,
    organizationId: UUID,
  ): GroupWithMemberCount?

  /**
   * Find all groups for a specific organization with member counts.
   *
   * @param organizationId The organization ID
   * @return List of groups with member counts
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm.user_id), 0) as member_count
    FROM "group" g
    LEFT JOIN group_member gm ON g.id = gm.group_id
    WHERE g.organization_id = :organizationId
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    """,
    nativeQuery = true,
  )
  fun findByOrganizationId(organizationId: UUID): List<GroupWithMemberCount>

  /**
   * Find groups by their IDs with member counts.
   *
   * @param ids Set of group IDs
   * @return List of groups with member counts
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm.user_id), 0) as member_count
    FROM "group" g
    LEFT JOIN group_member gm ON g.id = gm.group_id
    WHERE g.id IN (:ids)
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    """,
    nativeQuery = true,
  )
  fun findByIdIn(ids: Set<UUID>): List<GroupWithMemberCount>

  /**
   * Find all groups that a user belongs to with member counts.
   *
   * Note: This query uses two joins on group_member for different purposes:
   * - INNER JOIN (gm): Filters to only groups where the user is a member
   * - LEFT JOIN (gm2): Counts ALL members in those groups (not just the filtering user)
   * Both joins are necessary to achieve this combined filtering + counting logic.
   *
   * @param userId The user ID
   * @return List of groups with member counts
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm2.user_id), 0) as member_count
    FROM "group" g
    INNER JOIN group_member gm ON g.id = gm.group_id AND gm.user_id = :userId
    LEFT JOIN group_member gm2 ON g.id = gm2.group_id
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    """,
    nativeQuery = true,
  )
  fun findGroupsByUserId(userId: UUID): List<GroupWithMemberCount>

  /**
   * Find all groups for a specific organization with member counts.
   * This is the original paginated query with limit/offset parameters.
   *
   * @param organizationId The organization ID
   * @param limit Maximum number of results
   * @param offset Number of results to skip
   * @return List of groups with member counts
   */
  @Query(
    """
    SELECT g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at,
           COALESCE(COUNT(gm.user_id), 0) as member_count
    FROM "group" g
    LEFT JOIN group_member gm ON g.id = gm.group_id
    WHERE g.organization_id = :organizationId
    GROUP BY g.id, g.name, g.description, g.organization_id, g.created_at, g.updated_at
    LIMIT :limit OFFSET :offset
    """,
    nativeQuery = true,
  )
  fun findByOrganizationIdWithMemberCount(
    organizationId: UUID,
    limit: Int,
    offset: Int,
  ): List<GroupWithMemberCount>
}
