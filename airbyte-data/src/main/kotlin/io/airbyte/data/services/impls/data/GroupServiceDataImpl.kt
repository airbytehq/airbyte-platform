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
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupMembershipSource
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.PaginationParams
import io.airbyte.data.services.UserNotOrganizationMemberException
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.sql.SQLException
import java.time.OffsetDateTime
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
  private val permissionRepository: PermissionRepository,
  private val organizationRepository: OrganizationRepository,
  private val scimConfigurationRepository: ScimConfigurationRepository,
  private val scimResourceMappingRepository: ScimResourceMappingRepository,
) : GroupService {
  @Transactional("config")
  override fun createGroup(group: Group): Group = createGroup(group, GroupMutationOrigin.AIRBYTE)

  @Transactional("config")
  override fun createGroupForScim(
    configurationId: UUID,
    organizationId: OrganizationId,
    name: String,
  ): Group {
    val now = OffsetDateTime.now()
    return createGroup(
      Group(
        groupId = GroupId(UUID.randomUUID()),
        name = name,
        description = null,
        organizationId = organizationId,
        memberCount = 0,
        createdAt = now,
        updatedAt = now,
      ),
      GroupMutationOrigin.SCIM(configurationId),
    )
  }

  private fun createGroup(
    group: Group,
    origin: GroupMutationOrigin,
  ): Group {
    lockOrganization(group.organizationId)
    if (origin is GroupMutationOrigin.SCIM) {
      verifyEnabledScimConfiguration(origin.configurationId, group.organizationId)
    }
    if (groupRepository.existsByNameIgnoreCaseAndOrganizationId(group.name, group.organizationId.value)) {
      throw groupNameConflict(group)
    }

    val entity = group.toEntity()
    try {
      val saved = groupRepository.save(entity)
      val savedId = saved.id ?: throw IllegalStateException("Group did not save with an ID: $saved")
      return findScopedGroup(savedId, group.organizationId)
    } catch (e: DataAccessException) {
      if (isUniqueConstraintViolation(e)) {
        throw groupNameConflict(group)
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
  override fun updateGroup(group: Group): Group = updateGroup(group.groupId, group.organizationId, GroupMutationOrigin.AIRBYTE) { group }

  @Transactional("config")
  override fun updateGroupForScim(
    configurationId: UUID,
    organizationId: OrganizationId,
    groupId: GroupId,
    name: String,
  ): Group =
    updateGroup(groupId, organizationId, GroupMutationOrigin.SCIM(configurationId)) { current ->
      current.copy(name = name)
    }

  private fun updateGroup(
    groupId: GroupId,
    organizationId: OrganizationId,
    origin: GroupMutationOrigin,
    update: (Group) -> Group,
  ): Group {
    lockOrganization(organizationId)
    val current = findScopedGroup(groupId.value, organizationId)
    val updated = update(current)
    val isMappedGroup =
      if (origin is GroupMutationOrigin.SCIM) {
        verifyScimGroup(origin.configurationId, organizationId, groupId)
        false
      } else if (current.name != updated.name) {
        verifyAirbyteMutationAllowed(groupId, organizationId, GroupMutation.RENAME)
      } else {
        false
      }
    if (
      current.name != updated.name &&
      groupRepository.existsByNameIgnoreCaseAndOrganizationIdAndIdNot(updated.name, organizationId.value, groupId.value)
    ) {
      throw groupNameConflict(updated)
    }

    try {
      if (
        groupRepository.updateByIdAndOrganizationId(
          groupId.value,
          organizationId.value,
          updated.name,
          updated.description,
        ) != 1L
      ) {
        throw IllegalArgumentException("Group with id $groupId does not exist in organization $organizationId")
      }
      if (isMappedGroup) {
        scimResourceMappingRepository.touchGroup(groupId.value, organizationId.value)
      }
      return findScopedGroup(groupId.value, organizationId)
    } catch (e: DataAccessException) {
      if (isUniqueConstraintViolation(e)) {
        throw groupNameConflict(updated)
      }
      throw e
    }
  }

  @Transactional("config")
  override fun deleteGroup(
    groupId: GroupId,
    organizationId: OrganizationId,
  ) {
    lockOrganization(organizationId)
    requireGroup(groupId, organizationId)
    verifyAirbyteMutationAllowed(groupId, organizationId, GroupMutation.DELETE)
    groupMemberRepository.deleteByGroupIdAndOrganizationId(groupId.value, organizationId.value)
    if (groupRepository.deleteByIdAndOrganizationId(groupId.value, organizationId.value) != 1L) {
      throw IllegalArgumentException("Group with id $groupId does not exist in organization $organizationId")
    }
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
    organizationId: OrganizationId,
    source: GroupMembershipSource,
  ): GroupMember {
    lockOrganization(organizationId)
    requireGroup(groupId, organizationId)
    val isMappedGroup =
      when (source) {
        GroupMembershipSource.Manual -> verifyAirbyteMutationAllowed(groupId, organizationId, GroupMutation.MEMBERSHIP)
        is GroupMembershipSource.Scim -> {
          verifyScimGroup(source.configurationId, organizationId, groupId)
          false
        }
      }

    when (source) {
      GroupMembershipSource.Manual -> {
        val configuration = scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
        if (configuration?.enabled == true) {
          val mapping =
            scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(userId.value, organizationId.value)
          if (mapping?.userActive == false) {
            throw InactiveUserAccessException("Cannot add an inactive SCIM User to a Group")
          }
        }
        if (!permissionRepository.existsByUserIdAndOrganizationId(userId.value, organizationId.value)) {
          throw UserNotOrganizationMemberException("User $userId is not a member of organization $organizationId")
        }
      }

      is GroupMembershipSource.Scim -> {
        val mapping =
          scimResourceMappingRepository.findUserForUpdate(
            source.userMappingId,
            source.configurationId,
            organizationId.value,
          )
        val configuration = scimConfigurationRepository.findByOrganizationId(organizationId.value)
        if (
          mapping?.userId != userId.value ||
          mapping?.userActive != true ||
          configuration?.id != source.configurationId ||
          configuration?.enabled != true
        ) {
          throw InactiveUserAccessException("SCIM Group members must reference an active User mapping")
        }
      }
    }
    val entity =
      EntityGroupMember(
        id = UUID.randomUUID(),
        groupId = groupId.value,
        userId = userId.value,
      )

    try {
      groupMemberRepository.save(entity)
      // Fetch back with user info from join query
      val savedMember =
        groupMemberWithUserInfoRepository
          .findByGroupIdAndUserIdAndOrganizationId(groupId.value, userId.value, organizationId.value)
          ?: throw IllegalStateException("Failed to retrieve saved group member")
      if (isMappedGroup) {
        scimResourceMappingRepository.touchGroup(groupId.value, organizationId.value)
      }
      return savedMember.toConfigModel()
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
    organizationId: OrganizationId,
  ) {
    lockOrganization(organizationId)
    requireGroup(groupId, organizationId)
    val isMappedGroup = verifyAirbyteMutationAllowed(groupId, organizationId, GroupMutation.MEMBERSHIP)
    val deleted = groupMemberRepository.deleteByGroupIdAndUserIdAndOrganizationId(groupId.value, userId.value, organizationId.value)
    if (isMappedGroup && deleted > 0L) {
      scimResourceMappingRepository.touchGroup(groupId.value, organizationId.value)
    }
  }

  @Transactional("config")
  override fun replaceGroupMembersForScim(
    configurationId: UUID,
    organizationId: OrganizationId,
    groupId: GroupId,
    userIds: List<UserId>,
  ) {
    lockOrganization(organizationId)
    requireGroup(groupId, organizationId)
    verifyScimGroup(configurationId, organizationId, groupId)
    userIds.forEach { userId ->
      val mapping =
        scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(
          userId.value,
          organizationId.value,
        )
      if (mapping?.scimConfigurationId != configurationId || mapping.userActive != true) {
        throw InactiveUserAccessException(
          "SCIM Group members must reference active User mappings in the same configuration",
        )
      }
    }
    groupMemberRepository.deleteByGroupIdAndOrganizationId(groupId.value, organizationId.value)
    userIds.forEach { userId ->
      groupMemberRepository.save(
        EntityGroupMember(
          id = UUID.randomUUID(),
          groupId = groupId.value,
          userId = userId.value,
        ),
      )
    }
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
  ): Boolean =
    if (excludeGroupId == null) {
      !groupRepository.existsByNameIgnoreCaseAndOrganizationId(name, organizationId.value)
    } else {
      !groupRepository.existsByNameIgnoreCaseAndOrganizationIdAndIdNot(name, organizationId.value, excludeGroupId.value)
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

  private fun lockOrganization(organizationId: OrganizationId) {
    if (organizationRepository.findByIdForUpdate(organizationId.value).isEmpty) {
      throw IllegalArgumentException("Organization $organizationId does not exist")
    }
  }

  private fun findScopedGroup(
    groupId: UUID,
    organizationId: OrganizationId,
  ): Group =
    groupWithMemberCountRepository
      .findByIdAndOrganizationId(groupId, organizationId.value)
      .map { it.toConfigModel() }
      .orElseThrow { IllegalArgumentException("Group with id $groupId does not exist in organization $organizationId") }

  private fun requireGroup(
    groupId: GroupId,
    organizationId: OrganizationId,
  ) {
    if (!groupRepository.existsByIdAndOrganizationId(groupId.value, organizationId.value)) {
      throw IllegalArgumentException("Group with id $groupId does not exist in organization $organizationId")
    }
  }

  private fun verifyEnabledScimConfiguration(
    configurationId: UUID,
    organizationId: OrganizationId,
  ) {
    val configuration =
      scimConfigurationRepository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    if (configuration?.enabled != true) throw ScimAuthenticationException()
  }

  private fun verifyScimGroup(
    configurationId: UUID,
    organizationId: OrganizationId,
    groupId: GroupId,
  ) {
    val state = scimResourceMappingRepository.findGroupManagementState(groupId.value, organizationId.value)
    if (state?.enabled != true || state.scimConfigurationId != configurationId) {
      throw ScimAuthenticationException()
    }
  }

  private fun verifyAirbyteMutationAllowed(
    groupId: GroupId,
    organizationId: OrganizationId,
    mutation: GroupMutation,
  ): Boolean {
    val state = scimResourceMappingRepository.findGroupManagementState(groupId.value, organizationId.value) ?: return false
    if (mutation == GroupMutation.DELETE || state.enabled) {
      throw GroupManagedByScimException(
        when (mutation) {
          GroupMutation.DELETE -> "SCIM-mapped Groups cannot be deleted while their mapping exists"
          GroupMutation.RENAME -> "SCIM-managed Group names cannot be changed while SCIM is enabled"
          GroupMutation.MEMBERSHIP -> "SCIM-managed Group membership cannot be changed while SCIM is enabled"
        },
      )
    }
    return true
  }

  private fun groupNameConflict(group: Group): GroupNameNotUniqueException =
    GroupNameNotUniqueException(
      "Group with name '${group.name}' already exists in organization ${group.organizationId}",
    )

  private sealed interface GroupMutationOrigin {
    data object AIRBYTE : GroupMutationOrigin

    data class SCIM(
      val configurationId: UUID,
    ) : GroupMutationOrigin
  }

  private enum class GroupMutation {
    RENAME,
    DELETE,
    MEMBERSHIP,
  }
}
