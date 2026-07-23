/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Configs
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
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
  private val workspaceService: WorkspaceService,
  private val organizationService: OrganizationService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
  private val entitlementService: EntitlementService,
  private val airbyteEdition: Configs.AirbyteEdition,
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
    ensureGroupsEntitlement(group.organizationId)

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
    ensureGroupsEntitlement(group.organizationId)

    // Validate that the user has access to the workspace/organization they're trying to grant permissions for
    if (workspaceId != null) {
      // Validate the workspace exists
      val workspace =
        try {
          workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
        } catch (_: ConfigNotFoundException) {
          throw ResourceNotFoundProblem(
            ProblemResourceData()
              .resourceType("workspace")
              .resourceId(workspaceId.toString()),
          )
        }

      // Prevent cross-organization scope escalation: verify workspace belongs to same org as group
      if (workspace.organizationId != group.organizationId.value) {
        val badRequestProblem =
          BadRequestProblem(
            ProblemMessageData().message(
              "Cannot grant group permissions to workspace in different organization. " +
                "Group belongs to organization ${group.organizationId.value}, " +
                "but workspace belongs to organization ${workspace.organizationId}",
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

      // User must have workspace_admin or higher role for the target workspace
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withRef(AuthenticationId.WORKSPACE_ID, workspaceId.toString())
        .requireRole(AuthRoleConstants.WORKSPACE_ADMIN)
    } else if (organizationId != null) {
      // Validate the organization exists
      val organization = organizationService.getOrganization(organizationId)
      if (organization.isEmpty) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("organization")
            .resourceId(organizationId.toString()),
        )
      }

      // Prevent cross-organization scope escalation: verify target org matches group's org
      if (organizationId != group.organizationId.value) {
        val badRequestProblem =
          BadRequestProblem(
            ProblemMessageData().message(
              "Cannot grant group permissions to different organization. " +
                "Group belongs to organization ${group.organizationId.value}, " +
                "but permission targets organization $organizationId",
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

      // User must have organization_admin or higher role for the target organization
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withRef(AuthenticationId.ORGANIZATION_ID, organizationId.toString())
        .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    } else {
      val badRequestProblem =
        BadRequestProblem(
          ProblemMessageData().message("Workspace ID or Organization ID must be provided in order to create a group permission."),
        )
      trackingHelper.trackFailuresIfAny(
        GROUP_PERMISSIONS_PATH,
        POST,
        userId,
        badRequestProblem,
      )
      throw badRequestProblem
    }

    // Check for duplicate group permissions
    val permissionTypeEnum = Permission.PermissionType.valueOf(groupPermissionCreateRequest.permissionType.name)
    val isDuplicate =
      when {
        workspaceId != null -> permissionService.groupPermissionExistsForWorkspace(groupId, permissionTypeEnum, workspaceId)
        organizationId != null -> permissionService.groupPermissionExistsForOrganization(groupId, permissionTypeEnum, organizationId)
        else -> false
      }

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
    ensureGroupsEntitlement(group.organizationId)

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

  private fun ensureGroupsEntitlement(orgId: OrganizationId) {
    if (airbyteEdition == Configs.AirbyteEdition.ENTERPRISE) return
    entitlementService.ensureEntitled(orgId, GroupsEntitlement)
  }
}
