/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupMemberWithUserInfoRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.GroupWithMemberCountRepository
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
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.sql.SQLException
import java.util.UUID
import io.airbyte.data.repositories.entities.GroupMember as EntityGroupMember

/**
 * Data implementation of GroupService using JDBC repositories.
 */
@Singleton
open class GroupServiceDataImpl(
  private val groupRepository: GroupRepository,
  private val groupWithMemberCountRepository: GroupWithMemberCountRepository,
  private val groupMemberRepository: GroupMemberRepository,
  private val groupMemberWithUserInfoRepository: GroupMemberWithUserInfoRepository,
) : GroupService {
  @Transactional("config")
  override fun createGroup(group: Group): Group {
    // This will check if group name is unique within the organization
    val entity = group.toEntity()
    try {
      val saved = groupRepository.save(entity)
      val savedId = saved.id ?: throw IllegalStateException("Group did not save with an ID: $saved")
      // Fetch back with member count
      return groupWithMemberCountRepository
        .findById(savedId)
        .map { it.toConfigModel() }
        .orElseThrow { IllegalStateException("Failed to retrieve saved group") }
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

  @Transactional("config")
  override fun getGroup(groupId: GroupId): Group? =
    groupWithMemberCountRepository
      .findById(groupId.value)
      .map { it.toConfigModel() }
      .orElse(null)

  @Transactional("config")
  override fun updateGroup(group: Group): Group {
    // Check if the group exists
    if (!groupRepository.existsById(group.groupId.value)) {
      throw IllegalArgumentException("Group with id ${group.groupId} does not exist")
    }

    // This will check if the new name conflicts with another group in the organization
    val entity = group.toEntity()
    try {
      val updated = groupRepository.update(entity)
      val updatedId = updated.id ?: throw IllegalStateException("Group update did not return an ID: $updated")
      // Fetch back with member count
      return groupWithMemberCountRepository
        .findById(updatedId)
        .map { it.toConfigModel() }
        .orElseThrow { IllegalStateException("Failed to retrieve updated group") }
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

  @Transactional("config")
  override fun deleteGroup(groupId: GroupId) {
    // Delete all group memberships first (though CASCADE should handle this)
    groupMemberRepository.deleteByGroupId(groupId.value)
    // Delete the group
    groupRepository.deleteById(groupId.value)
  }

  @Transactional("config")
  override fun getGroupsForOrganization(
    organizationId: OrganizationId,
    paginationParams: PaginationParams?,
  ): List<Group> {
    if (paginationParams != null) {
      return groupWithMemberCountRepository
        .findByOrganizationIdWithMemberCount(
          organizationId.value,
          paginationParams.limit,
          paginationParams.offset,
        ).map { it.toConfigModel() }
    }

    return groupWithMemberCountRepository.findByOrganizationId(organizationId.value).map { it.toConfigModel() }
  }

  @Transactional("config")
  override fun addGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): GroupMember {
    // Create the membership entity
    val entity =
      EntityGroupMember(
        id = UUID.randomUUID(),
        groupId = groupId.value,
        userId = userId.value,
      )

    try {
      groupMemberRepository.save(entity)
      // Fetch back with user info from join query
      return (
        groupMemberWithUserInfoRepository
          .findByGroupIdAndUserId(groupId.value, userId.value)
          ?: throw IllegalStateException("Failed to retrieve saved group member")
      ).toConfigModel()
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

  @Transactional("config")
  override fun removeGroupMember(
    groupId: GroupId,
    userId: UserId,
  ) {
    groupMemberRepository.deleteByGroupIdAndUserId(groupId.value, userId.value)
  }

  @Transactional("config")
  override fun getGroupMembers(groupId: GroupId): List<GroupMember> =
    groupMemberWithUserInfoRepository.findByGroupId(groupId.value).map {
      it.toConfigModel()
    }

  @Transactional("config")
  override fun getGroupMembers(
    groupId: GroupId,
    paginationParams: PaginationParams?,
  ): List<GroupMember> {
    if (paginationParams != null) {
      return groupMemberWithUserInfoRepository
        .findByGroupIdWithPagination(
          groupId.value,
          paginationParams.limit,
          paginationParams.offset,
        ).map { it.toConfigModel() }
    }

    return groupMemberWithUserInfoRepository.findByGroupId(groupId.value).map { it.toConfigModel() }
  }

  @Transactional("config")
  override fun getGroupsForUser(userId: UserId): List<Group> =
    groupWithMemberCountRepository.findGroupsByUserId(userId.value).map {
      it.toConfigModel()
    }

  @Transactional("config")
  override fun isGroupMember(
    groupId: GroupId,
    userId: UserId,
  ): Boolean = groupMemberRepository.existsByGroupIdAndUserId(groupId.value, userId.value)

  @Transactional("config")
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
