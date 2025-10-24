/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import java.io.IOException

/**
 * Service for managing user groups within organizations.
 * Groups enable organizing users and assigning permissions at the group level.
 */
interface GroupService {
  /**
   * Creates a new group within an organization.
   * @throws GroupNameNotUniqueException if a group with the same name already exists in the organization
   * @throws IOException if there's an error persisting the group
   */
  fun createGroup(group: Group): Group

  /**
   * Retrieves a group by its ID.
   * @return get the group if found, null otherwise
   * @throws IOException if there's an error retrieving the group
   */
  fun getGroup(groupId: GroupId): Group?

  /**
   * Updates an existing group.
   * @throws GroupNameNotUniqueException if the new name conflicts with another group in the organization
   * @throws IOException if there's an error updating the group
   */
  fun updateGroup(group: Group): Group

  /**
   * Deletes a group and all its memberships.
   * @throws IOException if there's an error deleting the group
   */
  fun deleteGroup(groupId: GroupId)

  /**
   * Lists all groups within an organization.
   * @throws IOException if there's an error retrieving groups
   */
  fun getGroupsForOrganization(
    organizationId: OrganizationId,
    paginationParams: PaginationParams?,
  ): List<Group>

  /**
   * Adds a user to a group.
   * @throws AlreadyGroupMemberException if the user is already a member of the group
   * @throws IOException if there's an error persisting the membership
   */
  fun addGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): GroupMember

  /**
   * Removes a user from a group.
   * @throws IOException if there's an error removing the membership
   */
  fun removeGroupMember(
    groupId: GroupId,
    userId: UserId,
  )

  /**
   * Lists all members of a group.
   * @throws IOException if there's an error retrieving members
   */
  fun getGroupMembers(groupId: GroupId): List<GroupMember>

  /**
   * Lists all groups that a user belongs to.
   * @throws IOException if there's an error retrieving groups
   */
  fun getGroupsForUser(userId: UserId): List<Group>

  /**
   * Checks if a user is a member of a specific group.
   * @throws IOException if there's an error checking membership
   */
  fun isGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): Boolean

  /**
   * Checks if a group name is unique within an organization.
   * @throws IOException if there's an error checking uniqueness
   */
  fun isGroupNameUnique(
    organizationId: OrganizationId,
    name: String,
    excludeGroupId: GroupId? = null,
  ): Boolean
}

/**
 * Exception thrown when attempting to create or update a group with a name that already exists
 * in the organization.
 */
class GroupNameNotUniqueException(
  message: String,
) : Exception(message)

/**
 * Exception thrown when attempting to add a user to a group they're already a member of.
 */
class AlreadyGroupMemberException(
  message: String,
) : Exception(message)

/**
 * Pagination params is used to paginate returns from this service
 *
 * @property limit
 * @property offset
 * @constructor Create empty Pagination params
 */
data class PaginationParams(
  val limit: Int,
  val offset: Int,
)
