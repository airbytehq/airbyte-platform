/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import io.airbyte.api.client.model.generated.DestinationCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationReadList
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.client.model.generated.PartialDestinationUpdate
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.helpers.getActorDefinitionIdFromActorName
import io.airbyte.api.server.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.api.server.mappers.DestinationReadMapper
import io.airbyte.api.server.mappers.DestinationsResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.exceptions.HttpClientResponseException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface DestinationService {
  fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    destinationDefinitionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse

  fun getDestination(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse

  fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse

  fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse

  fun deleteDestination(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  )

  fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
    authorization: String?,
    userInfo: String?,
  ): DestinationsResponse?

  fun getDestinationSyncModes(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): List<DestinationSyncMode>

  fun getDestinationSyncModes(
    destinationResponse: DestinationResponse,
    authorization: String?,
    userInfo: String?,
  ): List<DestinationSyncMode>
}

@Singleton
@Secondary
class DestinationServiceImpl(private val configApiClient: ConfigApiClient, private val userService: UserService) : DestinationService {
  companion object {
    private val log = LoggerFactory.getLogger(DestinationServiceImpl::class.java)
  }

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  /**
   * Creates a destination.
   */
  override fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    destinationDefinitionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse {
    val destinationCreateOss = DestinationCreate()
    destinationCreateOss.name = destinationCreateRequest.name
    destinationCreateOss.destinationDefinitionId = destinationDefinitionId
    destinationCreateOss.workspaceId = destinationCreateRequest.workspaceId
    destinationCreateOss.connectionConfiguration = destinationCreateRequest.configuration

    val response =
      try {
        configApiClient.createDestination(destinationCreateOss, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for createDestination: ", e)
        e.response as HttpResponse<DestinationRead>
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    ConfigClientErrorHandler.handleError(response, destinationCreateRequest.workspaceId.toString())
    return DestinationReadMapper.from(response.body()!!)
  }

  /**
   * Gets a destination by ID.
   */
  override fun getDestination(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse {
    val destinationIdRequestBody = DestinationIdRequestBody()
    destinationIdRequestBody.destinationId = destinationId

    log.info("getDestination request: $destinationIdRequestBody")
    val response =
      try {
        configApiClient.getDestination(destinationIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getDestination: ", e)
        e.response as HttpResponse<DestinationRead>
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    ConfigClientErrorHandler.handleError(response, destinationId.toString())
    return DestinationReadMapper.from(response.body()!!)
  }

  /**
   * Updates a destination by ID.
   */
  override fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse {
    val destinationUpdate =
      DestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPutRequest.configuration)
        .name(destinationPutRequest.name)

    val response =
      try {
        configApiClient.updateDestination(destinationUpdate, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for updateDestination: ", e)
        e.response as HttpResponse<DestinationRead>
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    ConfigClientErrorHandler.handleError(response, destinationId.toString())
    return DestinationReadMapper.from(response.body()!!)
  }

  /**
   * Partially updates a destination with patch semantics.
   */
  override fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    authorization: String?,
    userInfo: String?,
  ): DestinationResponse {
    val partialDestinationUpdate =
      PartialDestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPatchRequest.configuration)
        .name(destinationPatchRequest.name)

    val response =
      try {
        configApiClient.partialUpdateDestination(partialDestinationUpdate, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for partialUpdateDestination: ", e)
        e.response as HttpResponse<DestinationRead>
      }
    ConfigClientErrorHandler.handleError(response, destinationId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return DestinationReadMapper.from(response.body()!!)
  }

  /**
   * Deletes updates a destination by ID.
   */
  override fun deleteDestination(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ) {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(connectionId)
    val response =
      try {
        configApiClient.deleteDestination(destinationIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for destination delete: ", e)
        e.response as HttpResponse<String>
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
  }

  /**
   * Lists destinations by workspace IDs or all destinations if no workspace IDs are provided.
   */
  override fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String?,
    userInfo: String?,
  ): DestinationsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(userInfo) }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery

    val response =
      try {
        configApiClient.listDestinationsForWorkspaces(listResourcesForWorkspacesRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for listWorkspaces: ", e)
        e.response as HttpResponse<DestinationReadList>
      }
    ConfigClientErrorHandler.handleError(response, workspaceIds.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return DestinationsResponseMapper.from(
      response.body()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }

  override fun getDestinationSyncModes(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): List<DestinationSyncMode> {
    val destinationResponse: DestinationResponse = getDestination(destinationId, authorization, userInfo)
    return getDestinationSyncModes(destinationResponse, authorization, userInfo)
  }

  override fun getDestinationSyncModes(
    destinationResponse: DestinationResponse,
    authorization: String?,
    userInfo: String?,
  ): List<DestinationSyncMode> {
    val destinationDefinitionId: UUID =
      getActorDefinitionIdFromActorName(DESTINATION_NAME_TO_DEFINITION_ID, destinationResponse.destinationType)
    val destinationDefinitionIdWithWorkspaceId = DestinationDefinitionIdWithWorkspaceId()
    destinationDefinitionIdWithWorkspaceId.destinationDefinitionId = destinationDefinitionId
    destinationDefinitionIdWithWorkspaceId.workspaceId = destinationResponse.workspaceId
    var response: HttpResponse<DestinationDefinitionSpecificationRead>
    try {
      response = configApiClient.getDestinationSpec(destinationDefinitionIdWithWorkspaceId, authorization, userInfo)
      log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for getDestinationSpec: ", e)
      response = e.response as HttpResponse<DestinationDefinitionSpecificationRead>
    }
    ConfigClientErrorHandler.handleError(response, destinationResponse.destinationId.toString())
    val destinationDefinitionSpecificationRead = response.body.get()
    return destinationDefinitionSpecificationRead.supportedDestinationSyncModes!!
  }
}
