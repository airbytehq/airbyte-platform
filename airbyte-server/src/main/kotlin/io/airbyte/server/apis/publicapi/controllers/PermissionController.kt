/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.errors.problems.BadRequestProblem
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicPermissionsApi
import io.airbyte.public_api.model.generated.PermissionCreateRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.PERMISSIONS_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.services.PermissionService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
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
    if (workspaceId != null) { // add a workspace level permission
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
        // current user should have workspace_admin or a higher role
        Scope.WORKSPACE,
        listOf(workspaceId.toString()),
        setOf(WorkspaceAuthRole.WORKSPACE_ADMIN),
      )
    } else if (organizationId != null) { // add an organization level permission
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
        // current user should have org_admin or a higher role
        Scope.ORGANIZATION,
        listOf(organizationId.toString()),
        setOf(OrganizationAuthRole.ORGANIZATION_ADMIN),
      )
    } else {
      throw BadRequestProblem("Workspace ID or Organization ID must be provided in order to create a permission.")
    }
    // process and monitor the request
    val permissionResponse: Any? =
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

    trackingHelper.trackSuccess(
      PERMISSIONS_PATH,
      POST,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(permissionResponse)
      .build()
  }
}
