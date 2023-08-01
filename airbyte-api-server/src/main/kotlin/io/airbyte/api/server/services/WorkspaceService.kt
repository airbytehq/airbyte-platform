/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.airbyte_api.model.generated.WorkspacesResponse
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.api.client.model.generated.WorkspaceReadList
import io.airbyte.api.server.constants.AIRBYTE_API_AUTH_HEADER_VALUE
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.mappers.WorkspaceResponseMapper
import io.airbyte.api.server.mappers.WorkspacesResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.exceptions.HttpClientResponseException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Objects
import java.util.UUID

interface WorkspaceService {
  fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest, userInfo: String?): WorkspaceResponse

  fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    userInfo: String?,
  ): WorkspaceResponse

  fun getWorkspace(workspaceId: UUID, userInfo: String?): WorkspaceResponse

  fun deleteWorkspace(workspaceId: UUID, userInfo: String?)

  fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,

    userInfo: String?,
  ): WorkspacesResponse
}

@Singleton
@Secondary
class WorkspaceServiceImpl(private val configApiClient: ConfigApiClient, private val userService: UserService) : WorkspaceService {

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  companion object {
    private val log = LoggerFactory.getLogger(WorkspaceServiceImpl::class.java)
  }

  /**
   * Creates a workspace.
   */
  override fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest, userInfo: String?): WorkspaceResponse {
    val workspaceCreate = WorkspaceCreate().name(workspaceCreateRequest.name)
    val workspaceReadHttpResponse = try {
      configApiClient.createWorkspace(workspaceCreate, System.getenv(AIRBYTE_API_AUTH_HEADER_VALUE))
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for createWorkspace: ", e)
      e.response as HttpResponse<WorkspaceRead>
    }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + workspaceReadHttpResponse.body)
    ConfigClientErrorHandler.handleError(workspaceReadHttpResponse, workspaceReadHttpResponse.body()?.workspaceId.toString())
    return WorkspaceResponseMapper.from(
      Objects.requireNonNull(
        workspaceReadHttpResponse.body() as WorkspaceRead,
      ),
    )
  }

  /**
   * No-op in OSS.
   */
  override fun updateWorkspace(workspaceId: UUID, workspaceUpdateRequest: WorkspaceUpdateRequest, userInfo: String?): WorkspaceResponse {
    // Update workspace in the cloud version of the airbyte API currently only supports name updates, but we don't have name updates in OSS.
    return WorkspaceResponse()
  }

  /**
   * Fetches a workspace by ID.
   */
  override fun getWorkspace(workspaceId: UUID, userInfo: String?): WorkspaceResponse {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val response = try {
      configApiClient.getWorkspace(workspaceIdRequestBody, userInfo)
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for getWorkspace: ", e)
      e.response as HttpResponse<WorkspaceRead>
    }
    ConfigClientErrorHandler.handleError(response, workspaceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return WorkspaceResponseMapper.from(response.body()!!)
  }

  /**
   * Deletes a workspace by ID.
   */
  override fun deleteWorkspace(workspaceId: UUID, userInfo: String?) {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val response = try {
      configApiClient.deleteWorkspace(workspaceIdRequestBody, userInfo)
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for deleteWorkspace: ", e)
      e.response as HttpResponse<Unit>
    }
    ConfigClientErrorHandler.handleError(response, workspaceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body)
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

    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(null, userInfo) }
    log.debug("Workspaces to query: $workspaceIdsToQuery")
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery
    val response = try {
      configApiClient.listWorkspaces(listResourcesForWorkspacesRequestBody, userInfo)
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for listWorkspaces: ", e)
      e.response as HttpResponse<WorkspaceReadList>
    }
    ConfigClientErrorHandler.handleError(response, workspaceIds.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return WorkspacesResponseMapper.from(
      response.body()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }
}
