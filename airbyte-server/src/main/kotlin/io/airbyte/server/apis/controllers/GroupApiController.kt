/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.GroupAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.GroupManagedByScimProblem
import io.airbyte.api.problems.throwable.generated.GroupMemberAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.server.generated.apis.GroupApi
import io.airbyte.api.server.generated.models.GroupCreate
import io.airbyte.api.server.generated.models.GroupIdRequestBody
import io.airbyte.api.server.generated.models.GroupMemberRead
import io.airbyte.api.server.generated.models.GroupMemberReadList
import io.airbyte.api.server.generated.models.GroupMemberRequestBody
import io.airbyte.api.server.generated.models.GroupRead
import io.airbyte.api.server.generated.models.GroupReadList
import io.airbyte.api.server.generated.models.GroupUpdate
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Group
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.UserNotOrganizationMemberException
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.server.apis.mappers.toGroupMemberRead
import io.airbyte.server.apis.mappers.toGroupRead
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.time.OffsetDateTime
import java.util.UUID

private const val GROUP_RESOURCE_TYPE = "group"
private const val USER_RESOURCE_TYPE = "user"

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class GroupApiController(
  private val groupService: GroupService,
  private val roleResolver: RoleResolver,
  private val groupsEntitlementHelper: GroupsEntitlementHelper,
) : GroupApi {
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun addGroupMember(groupMemberRequestBody: GroupMemberRequestBody): GroupMemberRead {
    val group = getAuthorizedGroup(groupMemberRequestBody.groupId)

    return try {
      groupService
        .addGroupMember(
          GroupId(groupMemberRequestBody.groupId),
          UserId(groupMemberRequestBody.userId),
          group.organizationId,
        ).toGroupMemberRead()
    } catch (e: AlreadyGroupMemberException) {
      throw GroupMemberAlreadyExistsProblem(ProblemMessageData().message(e.message))
    } catch (e: InactiveUserAccessException) {
      throw StateConflictProblem(ProblemMessageData().message(e.message))
    } catch (e: UserNotOrganizationMemberException) {
      throw ResourceNotFoundProblem(
        e.message,
        ProblemResourceData()
          .resourceType(USER_RESOURCE_TYPE)
          .resourceId(groupMemberRequestBody.userId.toString()),
      )
    } catch (e: GroupManagedByScimException) {
      throw GroupManagedByScimProblem(ProblemMessageData().message(e.message))
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createGroup(groupCreate: GroupCreate): GroupRead {
    val organizationId = OrganizationId(groupCreate.organizationId)
    groupsEntitlementHelper.ensureEntitled(organizationId)
    val now = OffsetDateTime.now()
    val group =
      try {
        Group(
          groupId = GroupId(UUID.randomUUID()),
          name = groupCreate.name,
          description = groupCreate.description,
          organizationId = organizationId,
          memberCount = null,
          createdAt = now,
          updatedAt = now,
        )
      } catch (e: IllegalArgumentException) {
        throw BadRequestProblem(ProblemMessageData().message(e.message))
      }

    return try {
      groupService.createGroup(group).toGroupRead()
    } catch (e: GroupNameNotUniqueException) {
      throw GroupAlreadyExistsProblem(ProblemMessageData().message(e.message))
    }
  }

  @Status(HttpStatus.NO_CONTENT)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteGroup(groupIdRequestBody: GroupIdRequestBody) {
    val group = getAuthorizedGroup(groupIdRequestBody.groupId)

    try {
      groupService.deleteGroup(group.groupId, group.organizationId)
    } catch (e: GroupManagedByScimException) {
      throw GroupManagedByScimProblem(ProblemMessageData().message(e.message))
    }
  }

  override fun getGroup(groupIdRequestBody: GroupIdRequestBody): GroupRead = getAuthorizedGroup(groupIdRequestBody.groupId).toGroupRead()

  override fun listGroupMembers(groupIdRequestBody: GroupIdRequestBody): GroupMemberReadList {
    getAuthorizedGroup(groupIdRequestBody.groupId)

    return GroupMemberReadList(
      members = groupService.getGroupMembers(GroupId(groupIdRequestBody.groupId)).map { it.toGroupMemberRead() },
    )
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  override fun listGroups(organizationIdRequestBody: OrganizationIdRequestBody): GroupReadList {
    val organizationId = OrganizationId(organizationIdRequestBody.organizationId)
    groupsEntitlementHelper.ensureEntitled(organizationId)

    return GroupReadList(
      groups =
        groupService
          .getGroupsForOrganization(organizationId, paginationParams = null)
          .sortedWith(compareBy(String.CASE_INSENSITIVE_ORDER) { it.name })
          .map { it.toGroupRead() },
    )
  }

  @Status(HttpStatus.NO_CONTENT)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun removeGroupMember(groupMemberRequestBody: GroupMemberRequestBody) {
    val group = getAuthorizedGroup(groupMemberRequestBody.groupId)

    try {
      groupService.removeGroupMember(
        GroupId(groupMemberRequestBody.groupId),
        UserId(groupMemberRequestBody.userId),
        group.organizationId,
      )
    } catch (e: GroupManagedByScimException) {
      throw GroupManagedByScimProblem(ProblemMessageData().message(e.message))
    }
  }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateGroup(groupUpdate: GroupUpdate): GroupRead {
    val existingGroup = getAuthorizedGroup(groupUpdate.groupId)
    val updatedGroup =
      try {
        existingGroup.copy(
          name = groupUpdate.name,
          description = groupUpdate.description,
        )
      } catch (e: IllegalArgumentException) {
        throw BadRequestProblem(ProblemMessageData().message(e.message))
      }

    return try {
      groupService.updateGroup(updatedGroup).toGroupRead()
    } catch (e: GroupNameNotUniqueException) {
      throw GroupAlreadyExistsProblem(ProblemMessageData().message(e.message))
    } catch (e: GroupManagedByScimException) {
      throw GroupManagedByScimProblem(ProblemMessageData().message(e.message))
    }
  }

  private fun getAuthorizedGroup(groupId: UUID): Group {
    val group =
      groupService.getGroup(GroupId(groupId))
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType(GROUP_RESOURCE_TYPE)
            .resourceId(groupId.toString()),
        )

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    groupsEntitlementHelper.ensureEntitled(group.organizationId)

    return group
  }
}
