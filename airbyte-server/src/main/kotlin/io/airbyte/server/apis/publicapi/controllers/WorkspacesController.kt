/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID
import io.airbyte.publicApi.server.generated.apis.PublicWorkspacesApi
import io.airbyte.publicApi.server.generated.models.WorkspaceCreateRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceOAuthCredentialsRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceUpdateRequest
import io.airbyte.server.apis.publicapi.constants.API_PATH
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

private val logger = KotlinLogging.logger {}

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkspacesController(
  protected val workspaceService: WorkspaceService,
  protected val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicWorkspacesApi {
  @Path("$WORKSPACES_PATH/{workspaceId}/oauthCredentials")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: String,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId,
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      UUID.fromString(workspaceId),
      workspaceOAuthCredentialsRequest,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response {
    // Now that we have orgs everywhere, ensure the user is at least an organization editor
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      Scope.ORGANIZATION,
      listOf(DEFAULT_ORGANIZATION_ID.toString()),
      setOf(OrganizationAuthRole.ORGANIZATION_EDITOR),
    )
    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest)
  }

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteWorkspace(workspaceId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId,
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerDeleteWorkspace(UUID.fromString(workspaceId))
  }

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetWorkspace(workspaceId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId,
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    return workspaceService.controllerGetWorkspace(UUID.fromString(workspaceId))
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListWorkspaces(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    logger.debug { "listing workspaces: $workspaceIds" }
    apiAuthorizationHelper.checkWorkspacesPermission(
      workspaceIds?.map { it.toString() } ?: emptyList(),
      Scope.WORKSPACES,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    return workspaceService.controllerListWorkspaces(
      workspaceIds ?: emptyList(),
      includeDeleted,
      limit,
      offset,
    )
  }

  @Patch
  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicUpdateWorkspace(
    workspaceId: String,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId,
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return workspaceService.controllerUpdateWorkspace(UUID.fromString(workspaceId), workspaceUpdateRequest)
  }
}
