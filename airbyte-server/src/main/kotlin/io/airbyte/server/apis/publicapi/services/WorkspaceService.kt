/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceUpdateName
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID
import io.airbyte.public_api.model.generated.WorkspaceCreateRequest
import io.airbyte.public_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.public_api.model.generated.WorkspaceResponse
import io.airbyte.public_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.public_api.model.generated.WorkspacesResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_PATH
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.WorkspaceResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.util.UUID

interface WorkspaceService {
  fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): WorkspaceResponse

  fun controllerCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response

  fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): WorkspaceResponse

  fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response

  fun getWorkspace(workspaceId: UUID): WorkspaceResponse

  fun controllerGetWorkspace(workspaceId: UUID): Response

  fun deleteWorkspace(workspaceId: UUID)

  fun controllerDeleteWorkspace(workspaceId: UUID): Response

  fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): WorkspacesResponse

  fun controllerListWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): Response

  fun controllerSetWorkspaceOverrideOAuthParams(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
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
  override fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): WorkspaceResponse {
    // For now this should always be true in OSS.
    val organizationId = DEFAULT_ORGANIZATION_ID

    val workspaceCreate =
      WorkspaceCreate().name(
        workspaceCreateRequest.name,
      ).email(currentUserService.currentUser.email).organizationId(organizationId)
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

  override fun controllerCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: WorkspaceResponse =
      trackingHelper.callWithTracker(
        { createWorkspace(workspaceCreateRequest) },
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
   * Updates a workspace name in OSS.
   */
  override fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): WorkspaceResponse {
    val workspaceUpdate =
      WorkspaceUpdateName().apply {
        this.name = workspaceUpdateRequest.name
        this.workspaceId = workspaceId
      }
    val result =
      kotlin.runCatching { workspacesHandler.updateWorkspaceName(workspaceUpdate) }
        .onFailure {
          log.error("Error for updateWorkspace", it)
          ConfigClientErrorHandler.handleError(it, workspaceId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return WorkspaceResponseMapper.from(result.getOrNull()!!)
  }

  override fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: Any =
      trackingHelper.callWithTracker(
        { updateWorkspace(workspaceId, workspaceUpdateRequest) },
        WORKSPACES_WITH_ID_PATH,
        PATCH,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_WITH_ID_PATH,
      PATCH,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Fetches a workspace by ID.
   */
  override fun getWorkspace(workspaceId: UUID): WorkspaceResponse {
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

  override fun controllerGetWorkspace(workspaceId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          getWorkspace(
            workspaceId,
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
  override fun deleteWorkspace(workspaceId: UUID) {
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

  override fun controllerDeleteWorkspace(workspaceId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          deleteWorkspace(
            workspaceId,
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
    return io.airbyte.server.apis.publicapi.mappers.WorkspacesResponseMapper.from(
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
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }
}
