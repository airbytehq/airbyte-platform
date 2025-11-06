/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.GroupMemberAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Configs
import io.airbyte.config.GroupMember
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PaginationParams
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.publicApi.server.generated.apis.PublicGroupMembersApi
import io.airbyte.publicApi.server.generated.models.GroupMemberAddRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.GROUP_MEMBERS_PATH
import io.airbyte.server.apis.publicapi.constants.GROUP_MEMBERS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.mappers.GroupMembersResponseMapper
import io.airbyte.server.apis.publicapi.mappers.toGroupMemberResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class GroupMembersController(
  private val groupService: GroupService,
  private val userPersistence: UserPersistence,
  private val organizationService: OrganizationService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
  private val entitlementService: EntitlementService,
  private val airbyteEdition: Configs.AirbyteEdition,
) : PublicGroupMembersApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListGroupMembers(
    groupId: UUID,
    limit: Int,
    offset: Int,
  ): Response {
    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Require organization admin or higher to view group members
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Ensure organization has groups feature entitlement
    ensureGroupsEntitlement(group.organizationId)

    val members =
      trackingHelper.callWithTracker(
        {
          groupService.getGroupMembers(GroupId(groupId), PaginationParams(limit, offset))
        },
        GROUP_MEMBERS_PATH,
        GET,
        currentUserService.getCurrentUser().userId,
      )

    val nextUrl =
      if (members.size >= limit) {
        "$GROUP_MEMBERS_PATH?limit=$limit&offset=${offset + limit}".replace("{groupId}", groupId.toString())
      } else {
        null
      }

    val previousUrl =
      if (offset > 0) {
        val previousOffset = maxOf(0, offset - limit)
        "$GROUP_MEMBERS_PATH?limit=$limit&offset=$previousOffset".replace("{groupId}", groupId.toString())
      } else {
        null
      }

    return Response
      .status(HttpStatus.OK.code)
      .entity(
        GroupMembersResponseMapper.from(
          members = members.map { it.toGroupMemberResponse() },
          next = nextUrl,
          previous = previousUrl,
        ),
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicAddGroupMember(
    groupId: UUID,
    groupMemberAddRequest: GroupMemberAddRequest,
  ): Response {
    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Require organization admin to add members to groups
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Check that the entitlement is working
    ensureGroupsEntitlement(group.organizationId)

    // Validate that the user exists
    val userExists = userPersistence.getUser(groupMemberAddRequest.userId).isPresent
    if (!userExists) {
      throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("user")
          .resourceId(groupMemberAddRequest.userId.toString()),
      )
    }

    // Validate that the user is a member of the organization that owns the group
    val userInOrganization = organizationService.isMember(groupMemberAddRequest.userId, group.organizationId.value)
    if (!userInOrganization) {
      throw ResourceNotFoundProblem(
        "User ${groupMemberAddRequest.userId} is not a member of organization ${group.organizationId}",
        ProblemResourceData()
          .resourceType("user")
          .resourceId(groupMemberAddRequest.userId.toString()),
      )
    }

    val member: GroupMember =
      try {
        trackingHelper.callWithTracker(
          {
            groupService.addGroupMember(GroupId(groupId), UserId(groupMemberAddRequest.userId))
          },
          GROUP_MEMBERS_PATH,
          POST,
          currentUserService.getCurrentUser().userId,
        )
      } catch (e: AlreadyGroupMemberException) {
        throw GroupMemberAlreadyExistsProblem(
          ProblemMessageData().message(e.message),
        )
      }

    return Response
      .status(HttpStatus.CREATED.code)
      .entity(
        member.toGroupMemberResponse(),
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicRemoveGroupMember(
    groupId: UUID,
    userId: UUID,
  ): Response {
    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )
    // Require organization admin to remove members from groups
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Check that the entitlement is working
    ensureGroupsEntitlement(group.organizationId)

    trackingHelper.callWithTracker(
      {
        groupService.removeGroupMember(GroupId(groupId), UserId(userId))
      },
      GROUP_MEMBERS_WITH_ID_PATH,
      DELETE,
      currentUserService.getCurrentUser().userId,
    )

    return Response.status(HttpStatus.NO_CONTENT.code).build()
  }

  private fun ensureGroupsEntitlement(orgId: OrganizationId) {
    if (airbyteEdition == Configs.AirbyteEdition.ENTERPRISE) return
    entitlementService.ensureEntitled(orgId, GroupsEntitlement)
  }
}
