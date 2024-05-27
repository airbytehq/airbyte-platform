/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicPermissionsApi
import io.airbyte.public_api.model.generated.PermissionCreateRequest
import io.airbyte.public_api.model.generated.PermissionUpdateRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.PERMISSIONS_PATH
import io.airbyte.server.apis.publicapi.constants.PERMISSIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.services.PermissionService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(PERMISSIONS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class PermissionController(
  private val permissionService: PermissionService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicPermissionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreatePermission(permissionCreateRequest: PermissionCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    val workspaceId: UUID? = permissionCreateRequest.workspaceId
    val organizationId: UUID? = permissionCreateRequest.organizationId
    // auth check before processing the request
    if (workspaceId != null) { // adding a workspace level permission
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
        // current user should have workspace_admin or a higher role
        Scope.WORKSPACE,
        listOf(workspaceId.toString()),
        setOf(WorkspaceAuthRole.WORKSPACE_ADMIN),
      )
    } else if (organizationId != null) { // adding an organization level permission
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
        // current user should have organization_admin or a higher role
        Scope.ORGANIZATION,
        listOf(organizationId.toString()),
        setOf(OrganizationAuthRole.ORGANIZATION_ADMIN),
      )
    } else {
      val badRequestProblem =
        BadRequestProblem(ProblemMessageData().message("Workspace ID or Organization ID must be provided in order to create a permission."))
      trackingHelper.trackFailuresIfAny(
        PERMISSIONS_PATH,
        POST,
        userId,
        badRequestProblem,
      )
      throw badRequestProblem
    }
    // process and monitor the request
    val permissionResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.createPermission(
            permissionCreateRequest,
          )
        },
        PERMISSIONS_PATH,
        POST,
        userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(permissionResponse)
      .build()
  }

  @Path("/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeletePermission(permissionId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have at least a workspace_admin role to delete a workspace level permission
      // or at least an organization_admin role to delete an organization level permission
      Scope.PERMISSION,
      listOf(permissionId.toString()),
      setOf(WorkspaceAuthRole.WORKSPACE_ADMIN, OrganizationAuthRole.ORGANIZATION_ADMIN),
    )
    // process and monitor the request
    trackingHelper.callWithTracker(
      {
        permissionService.deletePermission(permissionId)
      },
      PERMISSIONS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .build()
  }

  @Path("/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetPermission(permissionId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have either at least a workspace_reader role to get a workspace level permission
      // or at least an organization_read role to read an organization level permission
      Scope.PERMISSION,
      listOf(permissionId.toString()),
      setOf(WorkspaceAuthRole.WORKSPACE_READER, OrganizationAuthRole.ORGANIZATION_READER),
    )
    val permissionResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.getPermission(permissionId)
        },
        PERMISSIONS_WITH_ID_PATH,
        GET,
        userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(permissionResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListPermissionsByUserId(userId: UUID?): Response {
    val currentUserId: UUID = currentUserService.currentUser.userId
    val permissionUserId = userId ?: currentUserId // if userId is not provided, then use current user ID by default
    // auth check before processing the request
    if (currentUserId != permissionUserId) {
      // then current user has to be organization_admin to access another user's permissions
      // (assuming all users are in the same organization)
      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(currentUserId)
    }
    // process and monitor the request
    val permissionsResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.getPermissionsByUserId(permissionUserId)
        },
        PERMISSIONS_PATH,
        GET,
        currentUserId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(permissionsResponse)
      .build()
  }

  @Patch
  @Path("/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicUpdatePermission(
    permissionId: UUID,
    permissionUpdateRequest: PermissionUpdateRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have either at least a workspace_admin role to update a workspace level permission
      // or at least an organization_admin role to update an organization level permission
      Scope.PERMISSION,
      listOf(permissionId.toString()),
      setOf(WorkspaceAuthRole.WORKSPACE_ADMIN, OrganizationAuthRole.ORGANIZATION_ADMIN),
    )
    // process and monitor the request
    val updatePermissionResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.updatePermission(permissionId, permissionUpdateRequest)
        },
        PERMISSIONS_WITH_ID_PATH,
        PATCH,
        userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(updatePermissionResponse)
      .build()
  }
}
