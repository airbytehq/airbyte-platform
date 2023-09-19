/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.WorkspacesApi
import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.WORKSPACES_PATH
import io.airbyte.api.server.constants.WORKSPACES_WITH_ID_PATH
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.services.UserService
import io.airbyte.api.server.services.WorkspaceService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller(WORKSPACES_PATH)
class WorkspacesController(
  private val workspaceService: WorkspaceService,
  private val userService: UserService,
) : WorkspacesApi {
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,

    userInfo: String?,
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }

  override fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val workspaceResponse: WorkspaceResponse =
      TrackingHelper.callWithTracker(
        { workspaceService.createWorkspace(workspaceCreateRequest!!, userInfo) },
        WORKSPACES_PATH,
        POST,
        userId,
      ) as WorkspaceResponse
    TrackingHelper.trackSuccess(
      WORKSPACES_PATH,
      POST,
      userId,
      workspaceResponse.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  override fun deleteWorkspace(workspaceId: UUID?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val workspaceResponse: Any? = TrackingHelper.callWithTracker(
      {
        workspaceService.deleteWorkspace(
          workspaceId!!,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      WORKSPACES_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  override fun getWorkspace(workspaceId: UUID?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val workspaceResponse: Any? = TrackingHelper.callWithTracker(
      {
        workspaceService.getWorkspace(
          workspaceId!!,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      WORKSPACES_WITH_ID_PATH,
      GET,
      userId,
    )
    TrackingHelper.trackSuccess(
      WORKSPACES_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  override fun listWorkspaces(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,

    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val safeWorkspaceIds = workspaceIds ?: emptyList()

    val workspaces: Any? = TrackingHelper.callWithTracker(
      {
        workspaceService.listWorkspaces(
          safeWorkspaceIds,
          includeDeleted!!,
          limit!!,
          offset!!,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      WORKSPACES_PATH,
      GET,
      userId,
    )
    TrackingHelper.trackSuccess(
      WORKSPACES_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaces)
      .build()
  }

  override fun updateWorkspace(
    workspaceId: UUID?,
    workspaceUpdateRequest: WorkspaceUpdateRequest?,

    userInfo: String?,
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }
}
