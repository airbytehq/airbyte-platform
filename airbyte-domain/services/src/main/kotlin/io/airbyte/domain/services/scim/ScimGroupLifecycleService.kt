/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.data.repositories.ScimActiveUserRow
import io.airbyte.data.repositories.ScimGroupRow
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimGroupConflictException
import io.airbyte.domain.models.scim.ScimGroupFilterAttribute
import io.airbyte.domain.models.scim.ScimGroupFilterClause
import io.airbyte.domain.models.scim.ScimGroupInvalidMemberException
import io.airbyte.domain.models.scim.ScimGroupListPage
import io.airbyte.domain.models.scim.ScimGroupMember
import io.airbyte.domain.models.scim.ScimGroupNotFoundException
import io.airbyte.domain.models.scim.ScimGroupRead
import io.airbyte.domain.models.scim.ScimGroupWrite
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class ScimGroupLifecycleService(
  private val mappingRepository: ScimResourceMappingRepository,
  private val groupService: GroupService,
) {
  @Transactional("config")
  open fun create(
    configurationId: UUID,
    organizationId: UUID,
    input: ScimGroupWrite,
  ): ScimGroupRead {
    val members = activeUsers(configurationId, organizationId, input.memberIds)
    val group =
      try {
        groupService.createGroupForScim(configurationId, OrganizationId(organizationId), input.displayName)
      } catch (_: GroupNameNotUniqueException) {
        throw ScimGroupConflictException()
      }
    val groupId = group.groupId.value
    val mapping =
      saveMapping(
        ScimResourceMapping(
          scimConfigurationId = configurationId,
          organizationId = organizationId,
          resourceType = ScimResourceType.GROUP,
          groupId = groupId,
          externalId = input.externalId,
          attributes = JsonNodeFactory.instance.objectNode(),
        ),
      )
    try {
      groupService.replaceGroupMembersForScim(
        configurationId,
        OrganizationId(organizationId),
        GroupId(groupId),
        members.map { UserId(it.userId) },
      )
    } catch (_: InactiveUserAccessException) {
      throw ScimGroupInvalidMemberException()
    }
    return get(configurationId, organizationId, requireNotNull(mapping.id))
  }

  open fun get(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
  ): ScimGroupRead =
    mappingRepository.findGroup(id, configurationId, organizationId)?.let(::toRead)
      ?: throw ScimGroupNotFoundException()

  open fun list(
    configurationId: UUID,
    organizationId: UUID,
    filter: ScimGroupFilterClause?,
    offset: Long,
    limit: Int,
  ): ScimGroupListPage {
    val displayName = filter?.takeIf { it.attribute == ScimGroupFilterAttribute.DISPLAY_NAME }?.value
    val memberId = filter?.takeIf { it.attribute == ScimGroupFilterAttribute.MEMBER }?.value
    val totalResults =
      mappingRepository
        .countGroups(configurationId, organizationId, displayName, memberId)
        .coerceAtMost(Int.MAX_VALUE.toLong())
        .toInt()
    if (limit == 0 || offset >= totalResults) {
      return ScimGroupListPage(emptyList(), totalResults)
    }

    val rows = mappingRepository.findGroupsPage(configurationId, organizationId, displayName, memberId, offset, limit)
    val membersByGroupId =
      if (rows.isEmpty()) {
        emptyMap()
      } else {
        mappingRepository
          .findGroupMembersForGroups(configurationId, organizationId, rows.map(ScimGroupRow::groupId))
          .groupBy({ it.groupId }, { ScimGroupMember(it.id, it.userId, it.display) })
      }
    return ScimGroupListPage(
      resources = rows.map { toRead(it, membersByGroupId[it.groupId].orEmpty()) },
      totalResults = totalResults,
    )
  }

  @Transactional("config")
  open fun replace(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
    input: ScimGroupWrite,
  ): ScimGroupRead {
    val current = locked(id, configurationId, organizationId)
    val groupId = requireNotNull(current.groupId)
    val members = activeUsers(configurationId, organizationId, input.memberIds)
    try {
      groupService.updateGroupForScim(configurationId, OrganizationId(organizationId), GroupId(groupId), input.displayName)
      if (mappingRepository.updateGroup(id, configurationId, organizationId, input.externalId) != 1L) {
        throw ScimGroupNotFoundException()
      }
      try {
        groupService.replaceGroupMembersForScim(
          configurationId,
          OrganizationId(organizationId),
          GroupId(groupId),
          members.map { UserId(it.userId) },
        )
      } catch (_: InactiveUserAccessException) {
        throw ScimGroupInvalidMemberException()
      }
    } catch (_: GroupNameNotUniqueException) {
      throw ScimGroupConflictException()
    } catch (exception: DataAccessException) {
      if (isGroupUniquenessViolation(exception)) throw ScimGroupConflictException()
      throw exception
    }
    return get(configurationId, organizationId, id)
  }

  @Transactional("config")
  open fun delete(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
  ) {
    val current = locked(id, configurationId, organizationId)
    groupService.replaceGroupMembersForScim(
      configurationId,
      OrganizationId(organizationId),
      GroupId(requireNotNull(current.groupId)),
      emptyList(),
    )
    if (mappingRepository.deleteGroup(id, configurationId, organizationId) != 1L) {
      throw ScimGroupNotFoundException()
    }
  }

  open fun areActiveUsers(
    configurationId: UUID,
    organizationId: UUID,
    memberIds: Set<UUID>,
  ): Boolean =
    memberIds.isEmpty() ||
      mappingRepository
        .findActiveUsersByIds(configurationId, organizationId, memberIds)
        .mapTo(mutableSetOf()) { it.id } == memberIds

  private fun activeUsers(
    configurationId: UUID,
    organizationId: UUID,
    memberIds: List<UUID>,
  ): List<ScimActiveUserRow> {
    if (memberIds.isEmpty()) return emptyList()
    val resolved = mappingRepository.findActiveUsersByIds(configurationId, organizationId, memberIds)
    val byMappingId = resolved.associateBy { it.id }
    if (byMappingId.keys != memberIds.toSet()) throw ScimGroupInvalidMemberException()
    return memberIds.map(byMappingId::getValue)
  }

  private fun locked(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping =
    mappingRepository.findGroupForUpdate(id, configurationId, organizationId)
      ?: throw ScimGroupNotFoundException()

  private fun toRead(row: ScimGroupRow): ScimGroupRead =
    toRead(
      row,
      mappingRepository
        .findGroupMembers(row.scimConfigurationId, row.organizationId, row.groupId)
        .map { ScimGroupMember(it.id, it.userId, it.display) },
    )

  private fun toRead(
    row: ScimGroupRow,
    members: List<ScimGroupMember>,
  ): ScimGroupRead =
    ScimGroupRead(
      id = row.id,
      configurationId = row.scimConfigurationId,
      organizationId = row.organizationId,
      groupId = row.groupId,
      externalId = row.externalId,
      displayName = row.displayName,
      createdAt = row.createdAt,
      updatedAt = row.updatedAt,
      members = members,
    )

  private fun saveMapping(mapping: ScimResourceMapping): ScimResourceMapping =
    try {
      mappingRepository.save(mapping)
    } catch (exception: DataAccessException) {
      if (isGroupUniquenessViolation(exception)) throw ScimGroupConflictException()
      throw exception
    }

  private fun isGroupUniquenessViolation(exception: DataAccessException): Boolean =
    GROUP_UNIQUENESS_CONSTRAINTS.any { exception.message?.contains(it) == true }

  private companion object {
    val GROUP_UNIQUENESS_CONSTRAINTS =
      listOf(
        "group_organization_id_name_key",
        "scim_resource_mapping_external_id_key",
        "scim_resource_mapping_group_key",
      )
  }
}
