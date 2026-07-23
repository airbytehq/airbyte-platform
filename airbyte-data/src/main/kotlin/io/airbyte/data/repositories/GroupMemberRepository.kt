/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.GroupMember
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

/**
 * Repository for managing GroupMember entities.
 * Handles write operations (save, delete) and simple queries without user information.
 * For read operations that need user email and name,
 * use GroupMemberWithUserInfoRepository instead.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupMemberRepository : PageableRepository<GroupMember, UUID> {
  /**
   * Check if a user is a member of a group.
   *
   * @param groupId The group ID
   * @param userId The user ID
   * @return true if the user is a member of the group
   */
  fun existsByGroupIdAndUserId(
    groupId: UUID,
    userId: UUID,
  ): Boolean

  /**
   * Delete a specific group membership.
   *
   * @param groupId The group ID
   * @param userId The user ID
   */
  fun deleteByGroupIdAndUserId(
    groupId: UUID,
    userId: UUID,
  )

  @Query(
    """
    DELETE FROM group_member scoped_membership
    USING "group" scoped_group
    WHERE scoped_membership.group_id = :groupId
      AND scoped_membership.user_id = :userId
      AND scoped_membership.group_id = scoped_group.id
      AND scoped_group.organization_id = :organizationId
    """,
  )
  fun deleteByGroupIdAndUserIdAndOrganizationId(
    groupId: UUID,
    userId: UUID,
    organizationId: UUID,
  ): Long

  /**
   * Delete all memberships for a specific group.
   * This is called when a group is deleted (via CASCADE in DB, but can be called explicitly).
   *
   * @param groupId The group ID
   */
  fun deleteByGroupId(groupId: UUID)

  /**
   * Delete all group memberships for a specific user.
   * This is called when a user is deleted (via CASCADE in DB, but can be called explicitly).
   *
   * @param userId The user ID
   */
  fun deleteByUserId(userId: UUID)

  @Query(
    """
    DELETE FROM group_member scoped_membership
    USING "group" scoped_group
    WHERE scoped_membership.user_id = :userId
      AND scoped_membership.group_id = scoped_group.id
      AND scoped_group.organization_id = :organizationId
    """,
  )
  fun deleteByUserIdAndOrganizationId(
    userId: UUID,
    organizationId: UUID,
  ): Long

  @Query(
    """
    DELETE FROM group_member scoped_membership
    USING "group" scoped_group
    WHERE scoped_membership.group_id = :groupId
      AND scoped_membership.group_id = scoped_group.id
      AND scoped_group.organization_id = :organizationId
    """,
  )
  fun deleteByGroupIdAndOrganizationId(
    groupId: UUID,
    organizationId: UUID,
  ): Long

  /**
   * Get the count of members in a group.
   *
   * @param groupId The group ID
   * @return Number of members in the group
   */
  fun countByGroupId(groupId: UUID): Long

  /**
   * Get the count of groups a user belongs to.
   *
   * @param userId The user ID
   * @return Number of groups the user belongs to
   */
  fun countByUserId(userId: UUID): Long
}
