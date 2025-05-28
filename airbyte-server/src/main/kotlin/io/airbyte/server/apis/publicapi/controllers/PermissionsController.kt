/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.publicApi.server.generated.apis.PublicPermissionsApi
import io.airbyte.publicApi.server.generated.models.PermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.PermissionResponse
import io.airbyte.publicApi.server.generated.models.PermissionUpdateRequest
import io.airbyte.publicApi.server.generated.models.PermissionsResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.PERMISSIONS_PATH
import io.airbyte.server.apis.publicapi.constants.PERMISSIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.PermissionReadMapper
import io.airbyte.server.apis.publicapi.mappers.PermissionResponseReadMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

private val log = KotlinLogging.logger {}

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class PermissionsController(
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
  private val permissionHandler: PermissionHandler,
) : PublicPermissionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreatePermission(permissionCreateRequest: PermissionCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    val workspaceId: UUID? = permissionCreateRequest.workspaceId
    val organizationId: UUID? = permissionCreateRequest.organizationId
    // auth check before processing the request
    if (workspaceId != null) { // adding a workspace level permission
      // current user should have workspace_admin or a higher role
      roleResolver
        .Request()
        .withCurrentUser()
        .withRef(AuthenticationId.WORKSPACE_ID, workspaceId.toString())
        .requireRole(AuthRoleConstants.WORKSPACE_ADMIN)
    } else if (organizationId != null) { // adding an organization level permission
      // current user should have organization_admin or a higher role
      roleResolver
        .Request()
        .withCurrentUser()
        .withRef(AuthenticationId.ORGANIZATION_ID, organizationId.toString())
        .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
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
          doCreatePermission(
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
    val permission = permissionHandler.getPermissionById(UUID.fromString(permissionId))
    // current user should have at least a workspace_admin role to delete a workspace level permission
    // or at least an organization_admin role to delete an organization level permission
    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.PERMISSION_ID, permissionId)
      .requireRole(
        when {
          permission.workspaceId != null -> AuthRoleConstants.WORKSPACE_ADMIN
          permission.organizationId != null -> AuthRoleConstants.ORGANIZATION_ADMIN
          else -> AuthRoleConstants.INSTANCE_ADMIN
        },
      )

    // process and monitor the request
    trackingHelper.callWithTracker(
      {
        doDeletePermission(UUID.fromString(permissionId))
      },
      PERMISSIONS_WITH_ID_PATH,
      DELETE,
      currentUserService.currentUser.userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .build()
  }

  @Path("$PERMISSIONS_PATH/{permissionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetPermission(permissionId: String): Response {
    val permission = permissionHandler.getPermissionById(UUID.fromString(permissionId))

    // current user should have either at least a workspace_reader role to get a workspace level permission
    // or at least an organization_read role to read an organization level permission
    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.PERMISSION_ID, permissionId)
      .requireRole(
        when {
          permission.workspaceId != null -> AuthRoleConstants.WORKSPACE_READER
          permission.organizationId != null -> AuthRoleConstants.ORGANIZATION_READER
          else -> AuthRoleConstants.INSTANCE_ADMIN
        },
      )

    val permissionResponse =
      trackingHelper.callWithTracker(
        {
          doGetPermission(UUID.fromString(permissionId))
        },
        PERMISSIONS_WITH_ID_PATH,
        GET,
        currentUserService.currentUser.userId,
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
      roleResolver
        .Request()
        .withCurrentUser()
        .withRef(AuthenticationId.ORGANIZATION_ID, organizationId)
        .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

      permissionsResponse =
        trackingHelper.callWithTracker(
          {
            doGetPermissionsByUserInAnOrganization(permissionUserId, UUID.fromString(organizationId))
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
            doGetPermissionsByUserId(permissionUserId)
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
    val permission = permissionHandler.getPermissionById(UUID.fromString(permissionId))

    // current user should have either at least a workspace_admin role to update a workspace level permission
    // or at least an organization_admin role to update an organization level permission
    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.PERMISSION_ID, permissionId)
      .requireRole(
        when {
          permission.workspaceId != null -> AuthRoleConstants.WORKSPACE_ADMIN
          permission.organizationId != null -> AuthRoleConstants.ORGANIZATION_ADMIN
          else -> AuthRoleConstants.INSTANCE_ADMIN
        },
      )

    // process and monitor the request
    val updatePermissionResponse =
      trackingHelper.callWithTracker(
        {
          doUpdatePermission(UUID.fromString(permissionId), permissionUpdateRequest)
        },
        PERMISSIONS_WITH_ID_PATH,
        PATCH,
        currentUserService.currentUser.userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(updatePermissionResponse)
      .build()
  }

  /**
   * Creates a permission.
   */
  private fun doCreatePermission(permissionCreateRequest: PermissionCreateRequest): PermissionResponse {
    val permissionCreateOss = Permission()
    permissionCreateOss.permissionType = enumValueOf(permissionCreateRequest.permissionType.name)
    permissionCreateOss.userId = permissionCreateRequest.userId
    permissionCreateOss.organizationId = permissionCreateRequest.organizationId
    permissionCreateOss.workspaceId = permissionCreateRequest.workspaceId

    val result =
      runCatching { permissionHandler.createPermission(permissionCreateOss) }
        .onFailure {
          log.error(it) { "Error for createPermission" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return result.getOrThrow().toApi()
  }

  /**
   * Gets all permissions of a single user (by user ID).
   */
  private fun doGetPermissionsByUserId(userId: UUID): PermissionsResponse {
    val result =
      runCatching { permissionHandler.permissionReadListForUser(userId) }
        .onFailure {
          log.error(it) { "Error for getPermissionsByUserId" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    val permissionReadList = result.getOrThrow().permissions
    return PermissionsResponse(
      data = permissionReadList.mapNotNull { PermissionResponseReadMapper.from(it) },
    )
  }

  /**
   * Gets all permissions of a single user (by user ID) in a single organization.
   */
  private fun doGetPermissionsByUserInAnOrganization(
    userId: UUID,
    organizationId: UUID,
  ): PermissionsResponse {
    val result =
      runCatching { permissionHandler.listPermissionsByUserInAnOrganization(userId, organizationId) }
        .onFailure {
          log.error(it) { "Error for getPermissionsByUserInAnOrganization" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    val permissionReadList = result.getOrThrow().permissions
    return PermissionsResponse(data = permissionReadList.mapNotNull { PermissionResponseReadMapper.from(it) })
  }

  /**
   * Gets a permission.
   */
  private fun doGetPermission(permissionId: UUID): PermissionResponse {
    val permissionIdRequestBody = PermissionIdRequestBody()
    permissionIdRequestBody.permissionId = permissionId
    val result =
      runCatching { permissionHandler.getPermissionRead(permissionIdRequestBody) }
        .onFailure {
          log.error(it) { "Error for getPermission" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return PermissionReadMapper.from(result.getOrThrow())
  }

  /**
   * Updates a permission.
   */
  private fun doUpdatePermission(
    permissionId: UUID,
    permissionUpdateRequest: PermissionUpdateRequest,
  ): PermissionResponse {
    val permissionUpdate = PermissionUpdate()
    permissionUpdate.permissionId = permissionId
    permissionUpdate.permissionType = enumValueOf(permissionUpdateRequest.permissionType.name)
    val updatedPermission =
      runCatching {
        permissionHandler.updatePermission(permissionUpdate)
        val updatedPermission = permissionHandler.getPermissionRead(PermissionIdRequestBody().permissionId(permissionId))
        updatedPermission
      }.onFailure {
        log.error(it) { "Error for updatePermission" }
        ConfigClientErrorHandler.handleError(it)
      }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + updatedPermission }
    return PermissionReadMapper.from(updatedPermission.getOrThrow())
  }

  /**
   * Deletes a permission.
   */
  private fun doDeletePermission(permissionId: UUID) {
    val permissionIdRequestBody = PermissionIdRequestBody()
    permissionIdRequestBody.permissionId = permissionId
    val result =
      runCatching { permissionHandler.deletePermission(permissionIdRequestBody) }
        .onFailure {
          log.error(it) { "Error for deletePermission" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
  }
}

private fun Permission.toApi() =
  PermissionResponse(
    permissionId = this.permissionId,
    permissionType = enumValueOf(this.permissionType.name),
    userId = this.userId,
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
  )
