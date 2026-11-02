/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.GroupAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PaginationParams
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicGroupsApi
import io.airbyte.publicApi.server.generated.models.GroupCreateRequest
import io.airbyte.publicApi.server.generated.models.GroupResponse
import io.airbyte.publicApi.server.generated.models.GroupUpdateRequest
import io.airbyte.publicApi.server.generated.models.GroupsResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.GROUPS_PATH
import io.airbyte.server.apis.publicapi.constants.GROUPS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.mappers.applyToGroupDomainModel
import io.airbyte.server.apis.publicapi.mappers.toGroupDomainModel
import io.airbyte.server.apis.publicapi.mappers.toGroupResponse
import io.airbyte.server.auth.PublicGroupListRule
import io.airbyte.server.auth.PublicGroupSavedRule
import io.airbyte.server.auth.PublicOrganizationBodyAdminRule
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class GroupsController(
  private val groupService: GroupService,
  private val trackingHelper: TrackingHelper,
  private val currentUserService: CurrentUserService,
  private val groupsEntitlementHelper: GroupsEntitlementHelper,
) : PublicGroupsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicGroupListRule::class,
  )
  override fun publicListGroups(
    organizationId: UUID?,
    limit: Int,
    offset: Int,
  ): Response {
    // Require organizationId to be provided
    if (organizationId == null) {
      throw BadRequestProblem(
        ProblemMessageData().message("organizationId query parameter is required"),
      )
    }

    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(OrganizationId(organizationId))

    // Fetch limit + 1 to detect if more results exist, then take only limit for response
    val allGroups =
      trackingHelper.callWithTracker(
        {
          groupService
            .getGroupsForOrganization(
              organizationId = OrganizationId(organizationId),
              paginationParams = PaginationParams(limit + 1, offset),
            ).map { it.toGroupResponse() }
        },
        GROUPS_PATH,
        GET,
        currentUserService.getCurrentUser().userId,
      )

    val groups = allGroups.take(limit)

    val nextUrl =
      if (allGroups.size > limit) {
        "$GROUPS_PATH?organizationId=$organizationId&limit=$limit&offset=${offset + limit}"
      } else {
        null
      }

    val previousUrl =
      if (offset > 0) {
        val previousOffset = maxOf(0, offset - limit)
        "$GROUPS_PATH?organizationId=$organizationId&limit=$limit&offset=$previousOffset"
      } else {
        null
      }

    val groupsResponse =
      GroupsResponse(
        data = groups,
        next = nextUrl,
        previous = previousUrl,
      )

    return Response
      .status(HttpStatus.OK.code)
      .entity(
        groupsResponse,
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicOrganizationBodyAdminRule::class,
  )
  override fun publicCreateGroup(groupCreateRequest: GroupCreateRequest): Response {
    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(OrganizationId(groupCreateRequest.organizationId))

    val groupResponse: GroupResponse =
      trackingHelper.callWithTracker(
        {
          try {
            groupService
              .createGroup(
                groupCreateRequest.toGroupDomainModel(),
              ).toGroupResponse()
          } catch (e: GroupNameNotUniqueException) {
            throw GroupAlreadyExistsProblem(
              ProblemMessageData().message(e.message),
            )
          }
        },
        GROUPS_PATH,
        POST,
        currentUserService.getCurrentUser().userId,
      )

    return Response
      .status(HttpStatus.CREATED.code)
      .entity(
        groupResponse,
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicGroupSavedRule::class,
  )
  override fun publicGetGroup(groupId: UUID): Response {
    val group =
      groupService.getGroup(
        GroupId(groupId),
      ) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(OrganizationId(group.organizationId.value))

    val groupResponse: GroupResponse =
      trackingHelper.callWithTracker(
        {
          group.toGroupResponse()
        },
        GROUPS_WITH_ID_PATH,
        GET,
        currentUserService.getCurrentUser().userId,
      )

    return Response
      .status(HttpStatus.OK.code)
      .entity(
        groupResponse,
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicGroupSavedRule::class,
  )
  override fun publicUpdateGroup(
    groupId: UUID,
    groupUpdateRequest: GroupUpdateRequest,
  ): Response {
    val existingGroup =
      groupService.getGroup(
        GroupId(groupId),
      ) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    val groupResponse: GroupResponse =
      trackingHelper.callWithTracker(
        {
          try {
            groupService
              .updateGroup(
                groupUpdateRequest.applyToGroupDomainModel(existingGroup),
              ).toGroupResponse()
          } catch (e: GroupNameNotUniqueException) {
            throw GroupAlreadyExistsProblem(
              ProblemMessageData().message(e.message),
            )
          } catch (e: GroupManagedByScimException) {
            throw StateConflictProblem(ProblemMessageData().message(e.message))
          }
        },
        GROUPS_WITH_ID_PATH,
        PATCH,
        currentUserService.getCurrentUser().userId,
      )

    return Response
      .status(HttpStatus.OK.code)
      .entity(
        groupResponse,
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicGroupSavedRule::class,
  )
  override fun publicDeleteGroup(groupId: UUID): Response {
    val existingGroup =
      groupService.getGroup(
        GroupId(groupId),
      ) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    try {
      trackingHelper.callWithTracker(
        {
          groupService.deleteGroup(existingGroup.groupId, existingGroup.organizationId)
        },
        GROUPS_WITH_ID_PATH,
        DELETE,
        currentUserService.getCurrentUser().userId,
      )
    } catch (e: GroupManagedByScimException) {
      throw StateConflictProblem(ProblemMessageData().message(e.message))
    }

    return Response
      .status(HttpStatus.NO_CONTENT.code)
      .build()
  }
}
