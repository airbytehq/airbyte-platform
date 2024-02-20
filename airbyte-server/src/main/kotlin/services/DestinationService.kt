/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.PartialDestinationUpdate
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.helpers.getActorDefinitionIdFromActorName
import io.airbyte.api.server.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.api.server.mappers.DestinationReadMapper
import io.airbyte.api.server.mappers.DestinationsResponseMapper
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface DestinationService {
  fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    destinationDefinitionId: UUID,
    userInfo: String?,
  ): DestinationResponse

  fun getDestination(
    destinationId: UUID,
    userInfo: String?,
  ): DestinationResponse

  fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    userInfo: String?,
  ): DestinationResponse

  fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    userInfo: String?,
  ): DestinationResponse

  fun deleteDestination(
    connectionId: UUID,
    userInfo: String?,
  )

  fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
    userInfo: String?,
  ): DestinationsResponse?

  fun getDestinationSyncModes(
    destinationId: UUID,
    userInfo: String?,
  ): List<DestinationSyncMode>

  fun getDestinationSyncModes(
    destinationResponse: DestinationResponse,
    userInfo: String?,
  ): List<DestinationSyncMode>
}

@Singleton
@Secondary
class DestinationServiceImpl(
  private val userService: UserService,
  private val destinationHandler: DestinationHandler,
  private val connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
  private val currentUserService: CurrentUserService,
) : DestinationService {
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
    userInfo: String?,
  ): DestinationResponse {
    val destinationCreateOss = DestinationCreate()
    destinationCreateOss.name = destinationCreateRequest.name
    destinationCreateOss.destinationDefinitionId = destinationDefinitionId
    destinationCreateOss.workspaceId = destinationCreateRequest.workspaceId
    destinationCreateOss.connectionConfiguration = destinationCreateRequest.configuration

    val result =
      kotlin.runCatching {
        destinationHandler.createDestination(destinationCreateOss)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationCreateRequest.workspaceId.toString())
      }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Gets a destination by ID.
   */
  override fun getDestination(
    destinationId: UUID,
    userInfo: String?,
  ): DestinationResponse {
    val destinationIdRequestBody = DestinationIdRequestBody()
    destinationIdRequestBody.destinationId = destinationId

    val result =
      kotlin.runCatching {
        destinationHandler.getDestination(destinationIdRequestBody)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationId.toString())
      }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Updates a destination by ID.
   */
  override fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    userInfo: String?,
  ): DestinationResponse {
    val destinationUpdate =
      DestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPutRequest.configuration)
        .name(destinationPutRequest.name)

    val result =
      kotlin.runCatching {
        destinationHandler.updateDestination(destinationUpdate)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationId.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Partially updates a destination with patch semantics.
   */
  override fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    userInfo: String?,
  ): DestinationResponse {
    val partialDestinationUpdate =
      PartialDestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPatchRequest.configuration)
        .name(destinationPatchRequest.name)

    val result =
      kotlin.runCatching {
        destinationHandler.partialDestinationUpdate(partialDestinationUpdate)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationId.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Deletes updates a destination by ID.
   */
  override fun deleteDestination(
    destinationId: UUID,
    userInfo: String?,
  ) {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationId)
    val result =
      kotlin.runCatching {
        destinationHandler.deleteDestination(destinationIdRequestBody)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationId.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
  }

  /**
   * Lists destinations by workspace IDs or all destinations if no workspace IDs are provided.
   */
  override fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    userInfo: String?,
  ): DestinationsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery

    val result =
      kotlin.runCatching {
        destinationHandler.listDestinationsForWorkspaces(listResourcesForWorkspacesRequestBody)
      }.onFailure {
        log.error("Error while listing destinations for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, workspaceIds.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return DestinationsResponseMapper.from(
      result.getOrNull()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }

  override fun getDestinationSyncModes(
    destinationId: UUID,
    userInfo: String?,
  ): List<DestinationSyncMode> {
    val destinationResponse: DestinationResponse = getDestination(destinationId, userInfo)
    return getDestinationSyncModes(destinationResponse, userInfo)
  }

  override fun getDestinationSyncModes(
    destinationResponse: DestinationResponse,
    userInfo: String?,
  ): List<DestinationSyncMode> {
    val destinationDefinitionId: UUID =
      getActorDefinitionIdFromActorName(DESTINATION_NAME_TO_DEFINITION_ID, destinationResponse.destinationType)
    val destinationDefinitionIdWithWorkspaceId = DestinationDefinitionIdWithWorkspaceId()
    destinationDefinitionIdWithWorkspaceId.destinationDefinitionId = destinationDefinitionId
    destinationDefinitionIdWithWorkspaceId.workspaceId = destinationResponse.workspaceId

    val result =
      kotlin.runCatching {
        connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId)
      }.onFailure {
        log.error("Error while listing destinations for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, destinationDefinitionId.toString())
      }

    val destinationDefinitionSpecificationRead = result.getOrNull()!!
    return destinationDefinitionSpecificationRead.supportedDestinationSyncModes!!
  }
}
