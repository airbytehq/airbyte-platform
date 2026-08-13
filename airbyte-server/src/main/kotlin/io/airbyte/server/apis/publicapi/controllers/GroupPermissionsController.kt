/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.GroupId
import io.airbyte.publicApi.server.generated.apis.PublicGroupPermissionsApi
import io.airbyte.publicApi.server.generated.models.GroupPermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.GroupPermissionsResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.GROUP_PERMISSIONS_PATH
import io.airbyte.server.apis.publicapi.constants.GROUP_PERMISSIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.mappers.toGroupPermissionResponse
import io.airbyte.server.apis.publicapi.mappers.toGroupPermissions
import io.airbyte.server.apis.publicapi.mappers.toPermission
import io.airbyte.server.helpers.GroupPermissionValidator
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
open class GroupPermissionsController(
  private val groupService: GroupService,
  private val permissionService: PermissionService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
  private val groupsEntitlementHelper: GroupsEntitlementHelper,
  private val groupPermissionValidator: GroupPermissionValidator,
) : PublicGroupPermissionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListGroupPermissions(groupId: UUID): Response {
    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Require organization admin or higher to view group permissions
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(group.organizationId)

    val permissions: List<Permission> =
      trackingHelper.callWithTracker(
        {
          permissionService.getPermissionsByGroupId(groupId)
        },
        GROUP_PERMISSIONS_PATH,
        GET,
        currentUserService.getCurrentUser().userId,
      )

    return Response
      .status(HttpStatus.OK.code)
      .entity(
        GroupPermissionsResponse(
          data = permissions.toGroupPermissions(),
        ),
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateGroupPermission(
    groupId: UUID,
    groupPermissionCreateRequest: GroupPermissionCreateRequest,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId
    val workspaceId: UUID? = groupPermissionCreateRequest.workspaceId
    val organizationId: UUID? = groupPermissionCreateRequest.organizationId

    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Require organization admin to assign permissions to groups
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(group.organizationId)

    val permissionTypeEnum = Permission.PermissionType.valueOf(groupPermissionCreateRequest.permissionType.name)

    // Validate type/scope coherence and that the user has access to the workspace/organization
    // they're trying to grant permissions for
    try {
      groupPermissionValidator.validateScope(group, permissionTypeEnum, workspaceId, organizationId)
    } catch (e: BadRequestProblem) {
      trackingHelper.trackFailuresIfAny(GROUP_PERMISSIONS_PATH, POST, userId, e)
      throw e
    }

    // Check for duplicate group permissions
    val isDuplicate = groupPermissionValidator.isDuplicate(groupId, permissionTypeEnum, workspaceId, organizationId)

    if (isDuplicate) {
      val resourceType = if (workspaceId != null) "workspace" else "organization"
      val resourceId = workspaceId ?: organizationId
      val badRequestProblem =
        BadRequestProblem(
          ProblemMessageData().message(
            "Group already has ${groupPermissionCreateRequest.permissionType} permission for $resourceType $resourceId",
          ),
        )
      trackingHelper.trackFailuresIfAny(
        GROUP_PERMISSIONS_PATH,
        POST,
        userId,
        badRequestProblem,
      )
      throw badRequestProblem
    }

    val permission: Permission =
      trackingHelper.callWithTracker(
        {
          permissionService.createGroupPermission(groupPermissionCreateRequest.toPermission(groupId))
        },
        GROUP_PERMISSIONS_PATH,
        POST,
        userId,
      )

    return Response
      .status(HttpStatus.CREATED.code)
      .entity(
        permission.toGroupPermissionResponse(),
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteGroupPermission(
    groupId: UUID,
    permissionId: UUID,
  ): Response {
    val group =
      groupService.getGroup(GroupId(groupId)) ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("group")
          .resourceId(groupId.toString()),
      )

    // Require organization admin to remove permissions from groups
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    // Check that the entitlement is working
    groupsEntitlementHelper.ensureEntitled(group.organizationId)

    val permission: Permission =
      try {
        permissionService.getPermission(permissionId)
      } catch (_: ConfigNotFoundException) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("permission")
            .resourceId(permissionId.toString()),
        )
      }

    if (permission.groupId == null || permission.groupId != groupId) {
      throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType("permission")
          .resourceId(permissionId.toString()),
      )
    }

    trackingHelper.callWithTracker(
      {
        permissionService.deleteGroupPermission(permissionId)
      },
      GROUP_PERMISSIONS_WITH_ID_PATH,
      DELETE,
      currentUserService.getCurrentUser().userId,
    )
    return Response.status(HttpStatus.NO_CONTENT.code).build()
  }
}
