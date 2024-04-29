/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID
import io.airbyte.public_api.generated.PublicWorkspacesApi
import io.airbyte.public_api.model.generated.WorkspaceCreateRequest
import io.airbyte.public_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.public_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_PATH
import io.airbyte.server.apis.publicapi.services.WorkspaceService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

val logger = KotlinLogging.logger {}

@Controller(WORKSPACES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkspacesController(
  protected val workspaceService: WorkspaceService,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicWorkspacesApi {
  @Path("/{workspaceId}/oauthCredentials")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      workspaceId,
      workspaceOAuthCredentialsRequest,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest?): Response {
    // Now that we have orgs everywhere, ensure the user is at least an organization editor
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      Scope.ORGANIZATION,
      listOf(DEFAULT_ORGANIZATION_ID.toString()),
      setOf(OrganizationAuthRole.ORGANIZATION_EDITOR),
    )
    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest!!)
  }

  @Path("/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteWorkspace(workspaceId: UUID?): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerDeleteWorkspace(workspaceId)
  }

  @Path("/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetWorkspace(workspaceId: UUID?): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    return workspaceService.controllerGetWorkspace(workspaceId)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListWorkspaces(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    logger.debug { "listing workspaces: $workspaceIds" }
    apiAuthorizationHelper.checkWorkspacePermissions(
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
    )
  }

  @Patch
  @Path("/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicUpdateWorkspace(
    workspaceId: UUID?,
    workspaceUpdateRequest: WorkspaceUpdateRequest?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(workspaceId!!.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerUpdateWorkspace(workspaceId, workspaceUpdateRequest!!)
  }
}
