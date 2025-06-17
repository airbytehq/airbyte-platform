/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
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
  private val roleResolver: RoleResolver,
) : PublicWorkspacesApi {
  @Path("$WORKSPACES_PATH/{workspaceId}/oauthCredentials")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: String,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest,
  ): Response {
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    return workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      UUID.fromString(workspaceId),
      workspaceOAuthCredentialsRequest,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response {
    // This request is hard-coded to the DEFAULT_ORGANIZATION_ID in OSS,
    // because there's only one organization in OSS/SME.
    // This controller is overridden in Airbyte Cloud to allow multiple workspaces.
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, DEFAULT_ORGANIZATION_ID)
      .requireRole(AuthRoleConstants.ORGANIZATION_EDITOR)

    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest)
  }

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteWorkspace(workspaceId: String): Response {
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    return workspaceService.controllerDeleteWorkspace(UUID.fromString(workspaceId))
  }

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetWorkspace(workspaceId: String): Response {
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)
    return workspaceService.controllerGetWorkspace(UUID.fromString(workspaceId))
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListWorkspaces(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    // If workspace IDs were given, then verify the user has access to those workspaces.
    // If none were given, then the WorkspaceService will determine the workspaces for the current user.
    if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

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
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)
    return workspaceService.controllerUpdateWorkspace(UUID.fromString(workspaceId), workspaceUpdateRequest)
  }
}
