/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.GroupMemberWithUserInfo
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.UUID

/**
 * Read-only repository for querying GroupMembers with user information.
 * All queries include a JOIN with the user table to populate email and name fields.
 * Use GroupMemberRepository for write operations (save, delete).
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface GroupMemberWithUserInfoRepository : GenericRepository<GroupMemberWithUserInfo, UUID> {
  /**
   * Find all members of a specific group with user information.
   *
   * @param groupId The group ID
   * @return List of group memberships with user information
   */
  @Query(
    """
    SELECT gm.id, gm.group_id, gm.user_id, gm.created_at,
           u.email as email, u.name as name
    FROM group_member gm
    INNER JOIN "user" u ON gm.user_id = u.id
    WHERE gm.group_id = :groupId
    ORDER BY gm.created_at DESC
    """,
    nativeQuery = true,
  )
  fun findByGroupId(groupId: UUID): List<GroupMemberWithUserInfo>

  /**
   * Find members of a specific group with pagination and user information.
   *
   * @param groupId The group ID
   * @param limit Maximum number of results
   * @param offset Number of results to skip
   * @return List of group memberships with user information
   */
  @Query(
    """
    SELECT gm.id, gm.group_id, gm.user_id, gm.created_at,
           u.email as email, u.name as name
    FROM group_member gm
    INNER JOIN "user" u ON gm.user_id = u.id
    WHERE gm.group_id = :groupId
    ORDER BY gm.created_at DESC
    LIMIT :limit OFFSET :offset
    """,
    nativeQuery = true,
  )
  fun findByGroupIdWithPagination(
    groupId: UUID,
    limit: Int,
    offset: Int,
  ): List<GroupMemberWithUserInfo>

  /**
   * Find all groups that a user belongs to with user information.
   *
   * @param userId The user ID
   * @return List of group memberships with user information
   */
  @Query(
    """
    SELECT gm.id, gm.group_id, gm.user_id, gm.created_at,
           u.email as email, u.name as name
    FROM group_member gm
    INNER JOIN "user" u ON gm.user_id = u.id
    WHERE gm.user_id = :userId
    ORDER BY gm.created_at DESC
    """,
    nativeQuery = true,
  )
  fun findByUserId(userId: UUID): List<GroupMemberWithUserInfo>

  /**
   * Find a specific group membership with user information.
   *
   * @param groupId The group ID
   * @param userId The user ID
   * @return The membership with user details if it exists, null otherwise
   */
  @Query(
    """
    SELECT gm.id, gm.group_id, gm.user_id, gm.created_at,
           u.email as email, u.name as name
    FROM group_member gm
    INNER JOIN "user" u ON gm.user_id = u.id
    WHERE gm.group_id = :groupId AND gm.user_id = :userId
    """,
    nativeQuery = true,
  )
  fun findByGroupIdAndUserId(
    groupId: UUID,
    userId: UUID,
  ): GroupMemberWithUserInfo?
}
