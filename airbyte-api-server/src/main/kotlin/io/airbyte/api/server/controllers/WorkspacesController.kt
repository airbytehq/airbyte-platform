/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.WorkspacesApi
import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.server.constants.WORKSPACES_PATH
import io.airbyte.api.server.services.WorkspaceService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.PATCH
import javax.ws.rs.Path
import javax.ws.rs.core.Response

@Controller(WORKSPACES_PATH)
open class WorkspacesController(
  private val workspaceService: WorkspaceService,
) : WorkspacesApi {
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerSetWorkspaceOverrideOAuthParams(
      workspaceId!!,
      workspaceOAuthCredentialsRequest,
      authorization,
      userInfo,
    )
  }

  override fun createWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerCreateWorkspace(workspaceCreateRequest!!, authorization, userInfo)
  }

  override fun deleteWorkspace(
    workspaceId: UUID?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerDeleteWorkspace(workspaceId!!, authorization, userInfo)
  }

  override fun getWorkspace(
    workspaceId: UUID?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerGetWorkspace(workspaceId!!, authorization, userInfo)
  }

  override fun listWorkspaces(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerListWorkspaces(
      workspaceIds ?: emptyList(),
      includeDeleted!!,
      limit!!,
      offset!!,
      authorization,
      userInfo,
    )
  }

  @PATCH
  @Path("/{workspaceId}")
  override fun updateWorkspace(
    workspaceId: UUID?,
    workspaceUpdateRequest: WorkspaceUpdateRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return workspaceService.controllerUpdateWorkspace(workspaceId!!, workspaceUpdateRequest!!, authorization, userInfo)
  }
}
