/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package controllers

import authorization.AirbyteApiAuthorizationHelper
import authorization.Scope
import io.airbyte.airbyte_api.generated.WorkspacesApi
import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.server.constants.WORKSPACES_PATH
import io.airbyte.api.server.services.WorkspaceService
import io.airbyte.commons.server.support.CurrentUserService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID
import javax.ws.rs.Path
import javax.ws.rs.core.Response

val logger = KotlinLogging.logger {}

@Controller(WORKSPACES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkspacesController(
  private val workspaceService: WorkspaceService,
  private val airbyteApiAuthorizationHelper: AirbyteApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : WorkspacesApi {
  @Path("/{workspaceId}/oauthCredentials")
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      workspaceId,
      workspaceOAuthCredentialsRequest,
      userInfo,
    )
  }

  override fun createWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest?,
    userInfo: String?,
  ): Response {
    // As long as user is authenticated, they can proceed.
    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest!!, userInfo)
  }

  @Path("/{workspaceId}")
  override fun deleteWorkspace(
    workspaceId: UUID?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerDeleteWorkspace(workspaceId, userInfo)
  }

  @Path("/{workspaceId}")
  override fun getWorkspace(
    workspaceId: UUID?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    return workspaceService.controllerGetWorkspace(workspaceId, userInfo)
  }

  override fun listWorkspaces(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    logger.debug { "listing workspaces: $workspaceIds" }
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      workspaceIds?.map { it.toString() } ?: emptyList(),
      Scope.WORKSPACES,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    return workspaceService.controllerListWorkspaces(
      workspaceIds ?: emptyList(),
      includeDeleted!!,
      limit!!,
      offset!!,
      userInfo,
    )
  }

  @Patch
  @Path("/{workspaceId}")
  override fun updateWorkspace(
    workspaceId: UUID?,
    workspaceUpdateRequest: WorkspaceUpdateRequest?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerUpdateWorkspace(workspaceId, workspaceUpdateRequest!!, userInfo)
  }
}
