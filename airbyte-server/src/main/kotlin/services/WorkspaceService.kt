/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.airbyte_api.model.generated.WorkspacesResponse
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.WORKSPACES_PATH
import io.airbyte.api.server.constants.WORKSPACES_WITH_ID_PATH
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.mappers.WorkspaceResponseMapper
import io.airbyte.api.server.mappers.WorkspacesResponseMapper
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.ws.rs.core.Response

interface WorkspaceService {
  fun createWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest,
    userInfo: String?,
  ): WorkspaceResponse

  fun controllerCreateWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest,
    userInfo: String?,
  ): Response

  fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    userInfo: String?,
  ): WorkspaceResponse

  fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    userInfo: String?,
  ): Response

  fun getWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): WorkspaceResponse

  fun controllerGetWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): Response

  fun deleteWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  )

  fun controllerDeleteWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): Response

  fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
    userInfo: String?,
  ): WorkspacesResponse

  fun controllerListWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
    userInfo: String?,
  ): Response

  fun controllerSetWorkspaceOverrideOAuthParams(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
    userInfo: String?,
  ): Response
}

@Singleton
@Secondary
open class WorkspaceServiceImpl(
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
  private val workspacesHandler: WorkspacesHandler,
  @Value("\${airbyte.api.host}") open val publicApiHost: String,
  private val currentUserService: CurrentUserService,
) : WorkspaceService {
  companion object {
    private val log = LoggerFactory.getLogger(WorkspaceServiceImpl::class.java)
  }

  /**
   * Creates a workspace.
   */
  override fun createWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest,
    userInfo: String?,
  ): WorkspaceResponse {
    val workspaceCreate = WorkspaceCreate().name(workspaceCreateRequest.name)
    val result =
      kotlin.runCatching { workspacesHandler.createWorkspace(workspaceCreate) }
        .onFailure {
          log.error("Error for createWorkspace", it)
          ConfigClientErrorHandler.handleError(it, workspaceCreateRequest.name)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return WorkspaceResponseMapper.from(
      result.getOrNull()!!,
    )
  }

  override fun controllerCreateWorkspace(
    workspaceCreateRequest: WorkspaceCreateRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: WorkspaceResponse =
      trackingHelper.callWithTracker(
        { createWorkspace(workspaceCreateRequest, userInfo) },
        WORKSPACES_PATH,
        POST,
        userId,
      ) as WorkspaceResponse
    trackingHelper.trackSuccess(
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

  /**
   * No-op in OSS.
   */
  override fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    userInfo: String?,
  ): WorkspaceResponse {
    // Update workspace in the cloud version of the airbyte API currently only supports name updates, but we don't have name updates in OSS.
    return WorkspaceResponse()
  }

  override fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    userInfo: String?,
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }

  /**
   * Fetches a workspace by ID.
   */
  override fun getWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): WorkspaceResponse {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val result =
      kotlin.runCatching { workspacesHandler.getWorkspace(workspaceIdRequestBody) }
        .onFailure {
          log.error("Error for getWorkspace", it)
          ConfigClientErrorHandler.handleError(it, workspaceId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return WorkspaceResponseMapper.from(result.getOrNull()!!)
  }

  override fun controllerGetWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          getWorkspace(
            workspaceId,
            getLocalUserInfoIfNull(userInfo),
          )
        },
        WORKSPACES_WITH_ID_PATH,
        GET,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Deletes a workspace by ID.
   */
  override fun deleteWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ) {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val result =
      kotlin.runCatching { workspacesHandler.deleteWorkspace(workspaceIdRequestBody) }
        .onFailure {
          log.error("Error for deleteWorkspace", it)
          ConfigClientErrorHandler.handleError(it, workspaceId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
  }

  override fun controllerDeleteWorkspace(
    workspaceId: UUID,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          deleteWorkspace(
            workspaceId,
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

  /**
   * Lists a workspace by a set of IDs or all workspaces if no IDs are provided.
   */
  override fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    userInfo: String?,
  ): WorkspacesResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)

    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }
    log.debug("Workspaces to query: {}", workspaceIdsToQuery)
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery
    val result =
      kotlin.runCatching { workspacesHandler.listWorkspacesPaginated(listResourcesForWorkspacesRequestBody) }
        .onFailure {
          log.error("Error for listWorkspaces", it)
          ConfigClientErrorHandler.handleError(it, workspaceIds.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return WorkspacesResponseMapper.from(
      result.getOrNull()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost,
    )
  }

  override fun controllerListWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaces: Any? =
      trackingHelper.callWithTracker(
        {
          listWorkspaces(
            workspaceIds,
            includeDeleted,
            limit,
            offset,
            getLocalUserInfoIfNull(userInfo),
          )
        },
        WORKSPACES_PATH,
        GET,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaces)
      .build()
  }

  override fun controllerSetWorkspaceOverrideOAuthParams(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
    userInfo: String?,
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }
}
