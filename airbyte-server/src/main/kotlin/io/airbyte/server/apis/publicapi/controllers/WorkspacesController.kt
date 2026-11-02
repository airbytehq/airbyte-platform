/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.publicApi.server.generated.apis.PublicWorkspacesApi
import io.airbyte.publicApi.server.generated.models.ActorTypeEnum
import io.airbyte.publicApi.server.generated.models.WorkspaceCreateRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceOAuthCredentialsRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceUpdateRequest
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_PATH
import io.airbyte.server.apis.publicapi.services.WorkspaceService
import io.airbyte.server.auth.PublicWorkspaceCreateRule
import io.airbyte.server.auth.PublicWorkspaceListRule
import io.airbyte.server.auth.PublicWorkspaceReadRule
import io.airbyte.server.auth.PublicWorkspaceUpdateRule
import io.airbyte.server.auth.PublicWorkspaceWriteRule
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
) : PublicWorkspacesApi {
  @Path("$WORKSPACES_PATH/{workspaceId}/oauthCredentials")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceWriteRule::class,
  )
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: String,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest,
  ): Response =
    workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      UUID.fromString(workspaceId),
      workspaceOAuthCredentialsRequest,
    )

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceWriteRule::class,
  )
  override fun deleteWorkspaceOAuthCredentials(
    workspaceId: String,
    actorType: ActorTypeEnum,
    name: String,
  ): Response =
    workspaceService.controllerDeleteWorkspaceOverrideOAuthParams(
      UUID.fromString(workspaceId),
      actorType,
      name,
    )

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceCreateRule::class,
  )
  override fun publicCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response {
    // This request is hard-coded to the DEFAULT_ORGANIZATION_ID in OSS,
    // because there's only one organization in OSS.
    // This controller is overridden in Airbyte Cloud to allow multiple workspaces.

    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest)
  }

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceWriteRule::class,
  )
  override fun publicDeleteWorkspace(workspaceId: String): Response = workspaceService.controllerDeleteWorkspace(UUID.fromString(workspaceId))

  @Path("$WORKSPACES_PATH/{workspaceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceReadRule::class,
  )
  override fun publicGetWorkspace(workspaceId: String): Response = workspaceService.controllerGetWorkspace(UUID.fromString(workspaceId))

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceListRule::class,
  )
  override fun publicListWorkspaces(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    // If none were given, then the WorkspaceService will determine the workspaces for the current user.

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
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceUpdateRule::class,
  )
  override fun publicUpdateWorkspace(
    workspaceId: String,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response = workspaceService.controllerUpdateWorkspace(UUID.fromString(workspaceId), workspaceUpdateRequest)
}
