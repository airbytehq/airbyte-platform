/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.GroupMember
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

/**
 * Repository for managing GroupMember entities.
 * Provides operations for managing user-group membership relationships.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupMemberRepository : PageableRepository<GroupMember, UUID> {
  /**
   * Find all members of a specific group.
   *
   * @param groupId The group ID
   * @return List of group memberships for the group
   */
  fun findByGroupId(groupId: UUID): List<GroupMember>

  /**
   * Find all groups that a user belongs to.
   *
   * @param userId The user ID
   * @return List of group memberships for the user
   */
  fun findByUserId(userId: UUID): List<GroupMember>

  /**
   * Find a specific group membership.
   *
   * @param groupId The group ID
   * @param userId The user ID
   * @return Optional containing the membership if it exists
   */
  fun findByGroupIdAndUserId(
    groupId: UUID,
    userId: UUID,
  ): Optional<GroupMember>

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
