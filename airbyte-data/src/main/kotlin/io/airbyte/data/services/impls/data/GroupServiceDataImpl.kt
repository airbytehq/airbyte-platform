/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PaginationParams
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.data.model.Pageable
import jakarta.inject.Singleton
import java.sql.SQLException
import java.util.UUID

/**
 * Data implementation of GroupService using JDBC repositories.
 */
@Singleton
class GroupServiceDataImpl(
  private val groupRepository: GroupRepository,
  private val groupMemberRepository: GroupMemberRepository,
) : GroupService {
  override fun createGroup(group: Group): Group {
    // This will check if group name is unique within the organization
    val entity = group.toEntity()
    try {
      val saved = groupRepository.save(entity)
      return saved.toConfigModel()
    } catch (e: DataAccessException) {
      // Handle race condition: unique constraint violation from database
      if (isUniqueConstraintViolation(e)) {
        throw GroupNameNotUniqueException(
          "Group with name '${group.name}' already exists in organization ${group.organizationId}",
        )
      }
      throw e
    }
  }

  override fun getGroup(groupId: GroupId): Group? =
    groupRepository
      .findById(groupId.value)
      .map { it.toConfigModel() }
      .orElse(null)

  override fun updateGroup(group: Group): Group {
    // Check if the group exists
    if (!groupRepository.existsById(group.groupId)) {
      throw IllegalArgumentException("Group with id ${group.groupId} does not exist")
    }

    // This will check if the new name conflicts with another group in the organization
    val entity = group.toEntity()
    try {
      val updated = groupRepository.update(entity)
      return updated.toConfigModel()
    } catch (e: DataAccessException) {
      // Handle race condition: unique constraint violation from database
      if (isUniqueConstraintViolation(e)) {
        throw GroupNameNotUniqueException(
          "Group with name '${group.name}' already exists in organization ${group.organizationId}",
        )
      }
      throw e
    }
  }

  override fun deleteGroup(groupId: GroupId) {
    // Delete all group memberships first (though CASCADE should handle this)
    groupMemberRepository.deleteByGroupId(groupId.value)
    // Delete the group
    groupRepository.deleteById(groupId.value)
  }

  override fun getGroupsForOrganization(
    organizationId: OrganizationId,
    paginationParams: PaginationParams?,
  ): List<Group> {
    if (paginationParams != null) {
      return groupRepository
        .findByOrganizationId(
          organizationId.value,
          Pageable.from(
            calculatePageNumber(
              paginationParams.limit,
              paginationParams.offset,
            ),
            paginationParams.limit,
          ),
        ).content
        .map { it.toConfigModel() }
    }

    return groupRepository.findByOrganizationId(organizationId.value).map { it.toConfigModel() }
  }

  private fun calculatePageNumber(
    limit: Int,
    offset: Int,
  ): Int = Math.floor(offset.toDouble() / limit.toDouble()).toInt()

  override fun addGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): GroupMember {
    // Create the membership
    // This will check if user is already in the group
    val member =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = groupId.value,
        userId = userId.value,
        createdAt = java.time.OffsetDateTime.now(),
      )

    val entity = member.toEntity()
    try {
      val saved = groupMemberRepository.save(entity)
      return saved.toConfigModel()
    } catch (e: DataAccessException) {
      // Handle race condition: unique constraint violation from database
      if (isUniqueConstraintViolation(e)) {
        throw AlreadyGroupMemberException(
          "User $userId is already a member of group $groupId",
        )
      }
      throw e
    }
  }

  override fun removeGroupMember(
    groupId: GroupId,
    userId: UserId,
  ) {
    groupMemberRepository.deleteByGroupIdAndUserId(groupId.value, userId.value)
  }

  override fun getGroupMembers(groupId: GroupId): List<GroupMember> = groupMemberRepository.findByGroupId(groupId.value).map { it.toConfigModel() }

  override fun getGroupsForUser(userId: UserId): List<Group> = groupRepository.findGroupsByUserId(userId.value).map { it.toConfigModel() }

  override fun isGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): Boolean = groupMemberRepository.existsByGroupIdAndUserId(groupId.value, userId.value)

  override fun isGroupNameUnique(
    organizationId: OrganizationId,
    name: String,
    excludeGroupId: GroupId?,
  ): Boolean {
    val existingGroup = groupRepository.findByNameAndOrganizationId(name, organizationId.value)
    return when {
      existingGroup == null -> true
      excludeGroupId != null && existingGroup.id == excludeGroupId.value -> true
      else -> false
    }
  }

  /**
   * Checks if a DataAccessException is caused by a unique constraint violation.
   * This is used to detect race conditions where concurrent operations violate database constraints.
   *
   * Uses PostgreSQL SQL state code 23505 which indicates a unique_violation error.
   * This is more reliable than string matching as SQL state codes are standardized.
   */
  private fun isUniqueConstraintViolation(e: DataAccessException): Boolean {
    var cause: Throwable? = e.cause
    while (cause != null) {
      if (cause is SQLException && cause.sqlState == "23505") {
        return true
      }
      cause = cause.cause
    }
    return false
  }
}
