/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserFilterAttribute
import io.airbyte.domain.models.scim.ScimUserFilterClause
import io.airbyte.domain.models.scim.ScimUserGroup
import io.airbyte.domain.models.scim.ScimUserListPage
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.models.scim.ScimUserRead
import io.airbyte.domain.models.scim.ScimUserWrite
import io.micronaut.data.exceptions.DataAccessException
import jakarta.inject.Singleton
import java.util.Locale
import java.util.UUID

@Singleton
open class ScimUserLifecycleService(
  private val mappingRepository: ScimResourceMappingRepository,
  private val userRepository: ScimAirbyteUserRepository,
  private val permissionRepository: PermissionRepository,
  private val groupMemberRepository: GroupMemberRepository,
) {
  open fun create(
    configurationId: UUID,
    organizationId: UUID,
    input: ScimUserWrite,
  ): ScimUserRead {
    val existing = mappingRepository.findAllUsers(configurationId, organizationId)
    rejectCreateIdentifierMatches(existing, input)

    userRepository.acquireGlobalEmailLock(input.primaryEmail)
    val globalUsers = userRepository.findByEmailIgnoreCaseForUpdate(input.primaryEmail)
    if (globalUsers.size > 1) {
      throw ScimUserConflictException()
    }
    val user =
      globalUsers.singleOrNull()
        ?: userRepository.save(
          ScimAirbyteUser(
            name = seedGlobalName(input),
            email = input.primaryEmail,
          ),
        )
    if (mappingRepository.findUserByUserId(user.id, configurationId, organizationId) != null) {
      throw ScimUserConflictException()
    }
    val mapping =
      saveMapping(
        ScimResourceMapping(
          scimConfigurationId = configurationId,
          organizationId = organizationId,
          resourceType = ScimResourceType.USER,
          userId = user.id,
          externalId = input.externalId,
          userName = input.userName,
          primaryEmail = input.primaryEmail,
          userActive = input.active,
          attributes = input.attributes.deepCopy(),
        ),
      )
    if (input.active) {
      addBaselineAccess(user.id, organizationId)
    } else {
      cleanupAccess(user.id, organizationId)
    }
    return toRead(mapping)
  }

  private fun saveMapping(mapping: ScimResourceMapping): ScimResourceMapping =
    try {
      mappingRepository.save(mapping)
    } catch (exception: DataAccessException) {
      if (SCIM_USER_UNIQUENESS_CONSTRAINTS.any { exception.message?.contains(it) == true }) {
        throw ScimUserConflictException()
      }
      throw exception
    }

  open fun get(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
  ): ScimUserRead =
    mappingRepository.findUser(id, configurationId, organizationId)?.let(::toRead)
      ?: throw ScimUserNotFoundException()

  open fun list(
    configurationId: UUID,
    organizationId: UUID,
    filters: List<ScimUserFilterClause> = emptyList(),
    offset: Long,
    limit: Int,
  ): ScimUserListPage {
    val userNames = filters.filter { it.attribute == ScimUserFilterAttribute.USER_NAME }.map { it.value }.distinctBy { it.lowercase(Locale.ROOT) }
    val externalIds = filters.filter { it.attribute == ScimUserFilterAttribute.EXTERNAL_ID }.map { it.value }.distinct()
    val emails = filters.filter { it.attribute == ScimUserFilterAttribute.EMAIL }.map { it.value }.distinctBy { it.lowercase(Locale.ROOT) }
    val workEmails = filters.filter { it.attribute == ScimUserFilterAttribute.WORK_EMAIL }.map { it.value }.distinctBy { it.lowercase(Locale.ROOT) }
    if (userNames.size > 1 || externalIds.size > 1) {
      return ScimUserListPage(emptyList(), 0)
    }
    val totalResults =
      mappingRepository
        .countUsers(
          configurationId,
          organizationId,
          userNames.singleOrNull(),
          externalIds.singleOrNull(),
          emails,
          workEmails,
        ).coerceAtMost(Int.MAX_VALUE.toLong())
        .toInt()
    val resources =
      if (limit == 0 || offset >= totalResults) {
        emptyList()
      } else {
        mappingRepository
          .findUsersPage(
            configurationId,
            organizationId,
            userNames.singleOrNull(),
            externalIds.singleOrNull(),
            emails,
            workEmails,
            offset,
            limit,
          ).map { toRead(it, emptyList()) }
      }
    return ScimUserListPage(resources, totalResults)
  }

  open fun enrichGroups(
    configurationId: UUID,
    organizationId: UUID,
    users: List<ScimUserRead>,
  ): List<ScimUserRead> {
    if (users.isEmpty()) return emptyList()
    val groupsByUserId =
      mappingRepository
        .findGroupsForUsers(configurationId, organizationId, users.map(ScimUserRead::userId))
        .groupBy({ it.userId }, { ScimUserGroup(it.id, it.displayName) })
    return users.map { it.copy(groups = groupsByUserId[it.userId].orEmpty()) }
  }

  open fun replace(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
    input: ScimUserWrite,
  ): ScimUserRead {
    val current = locked(id, configurationId, organizationId)
    val transitions = if (current.userActive != input.active) listOf(input.active) else emptyList()
    return update(current, configurationId, organizationId, input, transitions)
  }

  open fun patch(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
    input: ScimUserWrite,
    activeTransitions: List<Boolean>,
  ): ScimUserRead {
    val current = locked(id, configurationId, organizationId)
    validateTransitions(current.userActive!!, input.active, activeTransitions)
    return update(current, configurationId, organizationId, input, activeTransitions)
  }

  open fun delete(
    configurationId: UUID,
    organizationId: UUID,
    id: UUID,
  ) {
    val current = locked(id, configurationId, organizationId)
    cleanupAccess(current.userId!!, organizationId)
    if (mappingRepository.deleteUser(id, configurationId, organizationId) != 1L) {
      throw ScimUserNotFoundException()
    }
  }

  private fun update(
    current: ScimResourceMapping,
    configurationId: UUID,
    organizationId: UUID,
    input: ScimUserWrite,
    transitions: List<Boolean>,
  ): ScimUserRead {
    rejectUpdateIdentifierMatches(
      mappingRepository.findAllUsers(configurationId, organizationId),
      current.id!!,
      input,
    )
    userRepository.acquireGlobalEmailLock(input.primaryEmail)
    val globalUsers = userRepository.findByEmailIgnoreCaseForUpdate(input.primaryEmail)
    if (globalUsers.size > 1 || globalUsers.singleOrNull()?.id?.let { it != current.userId } == true) {
      throw ScimUserConflictException()
    }
    val updated =
      try {
        mappingRepository.updateUser(
          current.id!!,
          configurationId,
          organizationId,
          input.userName,
          input.primaryEmail,
          input.externalId,
          input.active,
          input.attributes.toString(),
        )
      } catch (exception: DataAccessException) {
        if (SCIM_USER_UNIQUENESS_CONSTRAINTS.any { exception.message?.contains(it) == true }) {
          throw ScimUserConflictException()
        }
        throw exception
      }
    if (updated != 1L) {
      throw ScimUserNotFoundException()
    }

    transitions.forEach { active ->
      if (active) {
        addBaselineAccess(current.userId!!, organizationId)
      } else {
        cleanupAccess(current.userId!!, organizationId)
      }
    }
    return get(configurationId, organizationId, current.id!!)
  }

  private fun locked(
    id: UUID,
    configurationId: UUID,
    organizationId: UUID,
  ): ScimResourceMapping =
    mappingRepository.findUserForUpdate(id, configurationId, organizationId)
      ?: throw ScimUserNotFoundException()

  private fun addBaselineAccess(
    userId: UUID,
    organizationId: UUID,
  ) {
    if (!permissionRepository.existsByUserIdAndOrganizationId(userId, organizationId)) {
      permissionRepository.save(
        Permission(
          userId = userId,
          organizationId = organizationId,
          permissionType = PermissionType.organization_member,
        ),
      )
    }
  }

  private fun cleanupAccess(
    userId: UUID,
    organizationId: UUID,
  ) {
    permissionRepository.deleteByUserIdAndOrganizationId(userId, organizationId)
    permissionRepository.deleteWorkspacePermissionsByUserIdAndOrganizationId(userId, organizationId)
    groupMemberRepository.deleteByUserIdAndOrganizationId(userId, organizationId)
  }

  private fun toRead(mapping: ScimResourceMapping): ScimUserRead =
    toRead(
      mapping,
      mappingRepository
        .findGroupsForUser(mapping.scimConfigurationId, mapping.organizationId, requireNotNull(mapping.userId))
        .map { ScimUserGroup(it.id, it.displayName) },
    )

  private fun toRead(
    mapping: ScimResourceMapping,
    groups: List<ScimUserGroup>,
  ): ScimUserRead {
    val configurationId = mapping.scimConfigurationId
    val organizationId = mapping.organizationId
    val userId = requireNotNull(mapping.userId)
    return ScimUserRead(
      id = requireNotNull(mapping.id),
      configurationId = configurationId,
      organizationId = organizationId,
      userId = userId,
      externalId = mapping.externalId,
      userName = requireNotNull(mapping.userName),
      primaryEmail = requireNotNull(mapping.primaryEmail),
      active = requireNotNull(mapping.userActive),
      attributes = (mapping.attributes as com.fasterxml.jackson.databind.node.ObjectNode).deepCopy(),
      createdAt = requireNotNull(mapping.createdAt),
      updatedAt = requireNotNull(mapping.updatedAt),
      groups = groups,
    )
  }

  private fun rejectCreateIdentifierMatches(
    mappings: List<ScimResourceMapping>,
    input: ScimUserWrite,
  ) {
    if (mappings.any { identifiersMatch(it, input) }) {
      throw ScimUserConflictException()
    }
  }

  private fun rejectUpdateIdentifierMatches(
    mappings: List<ScimResourceMapping>,
    targetId: UUID,
    input: ScimUserWrite,
  ) {
    if (mappings.any { it.id != targetId && identifiersMatch(it, input) }) {
      throw ScimUserConflictException()
    }
  }

  private fun identifiersMatch(
    mapping: ScimResourceMapping,
    input: ScimUserWrite,
  ): Boolean =
    mapping.userName.equals(input.userName, ignoreCase = true) ||
      mapping.primaryEmail.equals(input.primaryEmail, ignoreCase = true) ||
      (input.externalId != null && mapping.externalId == input.externalId)

  private fun seedGlobalName(input: ScimUserWrite): String {
    val name = input.attributes.path("name")
    val seed =
      name.path("formatted").textValue()?.takeIf(String::isNotBlank)
        ?: listOfNotNull(name.path("givenName").textValue(), name.path("familyName").textValue()).joinToString(" ").takeIf(String::isNotBlank)
        ?: input.attributes
          .path("displayName")
          .textValue()
          ?.takeIf(String::isNotBlank)
        ?: input.userName
    val codePointCount = seed.codePointCount(0, seed.length)
    val endIndex = seed.offsetByCodePoints(0, minOf(codePointCount, GLOBAL_USER_NAME_MAX_LENGTH))
    return seed.substring(0, endIndex)
  }

  private fun validateTransitions(
    initial: Boolean,
    final: Boolean,
    transitions: List<Boolean>,
  ) {
    var current = initial
    transitions.forEach { next ->
      require(next != current) { "SCIM active transitions must change the current state" }
      current = next
    }
    require(current == final) { "SCIM active transitions must end at the persisted state" }
  }

  private companion object {
    const val GLOBAL_USER_NAME_MAX_LENGTH = 256

    val SCIM_USER_UNIQUENESS_CONSTRAINTS =
      listOf(
        "scim_resource_mapping_configuration_user_key",
        "scim_resource_mapping_external_id_key",
        "scim_resource_mapping_primary_email_key",
        "scim_resource_mapping_user_name_key",
      )
  }
}
