/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.publicApi.server.generated.apis.PublicPermissionsApi
import io.airbyte.publicApi.server.generated.models.PermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.PermissionUpdateRequest
import io.airbyte.publicApi.server.generated.models.PermissionsResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
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

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class PermissionsController(
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

  @Path("$PERMISSIONS_PATH/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeletePermission(permissionId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have at least a workspace_admin role to delete a workspace level permission
      // or at least an organization_admin role to delete an organization level permission
      Scope.PERMISSION,
      listOf(permissionId),
      setOf(WorkspaceAuthRole.WORKSPACE_ADMIN, OrganizationAuthRole.ORGANIZATION_ADMIN),
    )
    // process and monitor the request
    trackingHelper.callWithTracker(
      {
        permissionService.deletePermission(UUID.fromString(permissionId))
      },
      PERMISSIONS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .build()
  }

  @Path("$PERMISSIONS_PATH/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetPermission(permissionId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have either at least a workspace_reader role to get a workspace level permission
      // or at least an organization_read role to read an organization level permission
      Scope.PERMISSION,
      listOf(permissionId),
      setOf(WorkspaceAuthRole.WORKSPACE_READER, OrganizationAuthRole.ORGANIZATION_READER),
    )
    val permissionResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.getPermission(UUID.fromString(permissionId))
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
  override fun publicListPermissionsByUserId(
    userId: String?,
    organizationId: String?,
  ): Response {
    val currentUserId: UUID = currentUserService.currentUser.userId
    val permissionUserId = userId?.let { UUID.fromString(it) } ?: currentUserId // if userId is not provided, then use current user ID by default
    val permissionsResponse: PermissionsResponse
    // get someone else's permissions
    if (currentUserId != permissionUserId) {
      if (organizationId == null) {
        val badRequestProblem =
          BadRequestProblem(ProblemMessageData().message("Organization ID is required when getting someone else's permissions."))
        trackingHelper.trackFailuresIfAny(
          PERMISSIONS_PATH,
          POST,
          currentUserId,
          badRequestProblem,
        )
        throw badRequestProblem
      }
      // Make sure current user has to be organization_admin to access another user's permissions.
      apiAuthorizationHelper.isUserOrganizationAdminOrThrow(currentUserId, UUID.fromString(organizationId))
      permissionsResponse =
        trackingHelper.callWithTracker(
          {
            permissionService.getPermissionsByUserInAnOrganization(permissionUserId, UUID.fromString(organizationId))
          },
          PERMISSIONS_PATH,
          GET,
          currentUserId,
        )
    } else {
      // Get all self permissions, ignoring the input `organizationId`.
      permissionsResponse =
        trackingHelper.callWithTracker(
          {
            permissionService.getPermissionsByUserId(permissionUserId)
          },
          PERMISSIONS_PATH,
          GET,
          currentUserId,
        )
    }
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(permissionsResponse)
      .build()
  }

  @Patch
  @Path("$PERMISSIONS_PATH/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicUpdatePermission(
    permissionId: String,
    permissionUpdateRequest: PermissionUpdateRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    // auth check before processing the request
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      // current user should have either at least a workspace_admin role to update a workspace level permission
      // or at least an organization_admin role to update an organization level permission
      Scope.PERMISSION,
      listOf(permissionId),
      setOf(WorkspaceAuthRole.WORKSPACE_ADMIN, OrganizationAuthRole.ORGANIZATION_ADMIN),
    )
    // process and monitor the request
    val updatePermissionResponse =
      trackingHelper.callWithTracker(
        {
          permissionService.updatePermission(UUID.fromString(permissionId), permissionUpdateRequest)
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
