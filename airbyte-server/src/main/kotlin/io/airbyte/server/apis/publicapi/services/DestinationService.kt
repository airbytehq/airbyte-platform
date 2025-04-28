/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.PartialDestinationUpdate
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.models.DestinationCreateRequest
import io.airbyte.publicApi.server.generated.models.DestinationPatchRequest
import io.airbyte.publicApi.server.generated.models.DestinationPutRequest
import io.airbyte.publicApi.server.generated.models.DestinationResponse
import io.airbyte.publicApi.server.generated.models.DestinationsResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.helpers.toInternal
import io.airbyte.server.apis.publicapi.mappers.DestinationReadMapper
import io.airbyte.server.apis.publicapi.mappers.DestinationsResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface DestinationService {
  fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    destinationDefinitionId: UUID,
  ): DestinationResponse

  fun getDestination(
    destinationId: UUID,
    includeSecretCoordinates: Boolean?,
  ): DestinationResponse

  fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
  ): DestinationResponse

  fun partialUpdateDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
  ): DestinationResponse

  fun deleteDestination(destinationId: UUID)

  fun listDestinationsForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): DestinationsResponse?

  fun getDestinationRead(destinationId: UUID): DestinationRead

  fun getDestinationSyncModes(destinationId: UUID): List<DestinationSyncMode>

  fun getDestinationSyncModes(destinationRead: DestinationRead): List<DestinationSyncMode>
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
  ): DestinationResponse {
    val destinationCreateOss = DestinationCreate()
    destinationCreateOss.name = destinationCreateRequest.name
    destinationCreateOss.destinationDefinitionId = destinationDefinitionId
    destinationCreateOss.workspaceId = destinationCreateRequest.workspaceId
    destinationCreateOss.connectionConfiguration = destinationCreateRequest.configuration
    destinationCreateOss.resourceAllocation = destinationCreateRequest.resourceAllocation?.toInternal()

    val result =
      kotlin
        .runCatching {
          destinationHandler.createDestination(destinationCreateOss)
        }.onFailure {
          log.error("Error while listing connections for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
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
    includeSecretCoordinates: Boolean?,
  ): DestinationResponse {
    val destinationIdRequestBody = DestinationIdRequestBody()
    destinationIdRequestBody.destinationId = destinationId

    val result =
      kotlin
        .runCatching {
          destinationHandler.getDestination(destinationIdRequestBody, includeSecretCoordinates == true)
        }.onFailure {
          log.error("Error while getting destination: ", it)
          ConfigClientErrorHandler.handleError(it)
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Gets a DestinationRead by ID.
   */
  override fun getDestinationRead(destinationId: UUID): DestinationRead {
    val destinationIdRequestBody = DestinationIdRequestBody()
    destinationIdRequestBody.destinationId = destinationId

    val result =
      runCatching {
        destinationHandler.getDestination(destinationIdRequestBody)
      }.onFailure {
        log.error("Error while getting destination: ", it)
        ConfigClientErrorHandler.handleError(it)
      }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrThrow()
    return destinationRead
  }

  /**
   * Updates a destination by ID.
   */
  override fun updateDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
  ): DestinationResponse {
    val destinationUpdate =
      DestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPutRequest.configuration)
        .name(destinationPutRequest.name)
        .resourceAllocation(destinationPutRequest.resourceAllocation?.toInternal())

    val result =
      kotlin
        .runCatching {
          destinationHandler.updateDestination(destinationUpdate)
        }.onFailure {
          log.error("Error while listing connections for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
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
  ): DestinationResponse {
    val partialDestinationUpdate =
      PartialDestinationUpdate()
        .destinationId(destinationId)
        .connectionConfiguration(destinationPatchRequest.configuration)
        .name(destinationPatchRequest.name)
        .resourceAllocation(destinationPatchRequest.resourceAllocation?.toInternal())

    val result =
      kotlin
        .runCatching {
          destinationHandler.partialDestinationUpdate(partialDestinationUpdate)
        }.onFailure {
          log.error("Error while listing connections for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val destinationRead = result.getOrNull()!!
    return DestinationReadMapper.from(destinationRead)
  }

  /**
   * Deletes updates a destination by ID.
   */
  override fun deleteDestination(destinationId: UUID) {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationId)
    val result =
      kotlin
        .runCatching {
          destinationHandler.deleteDestination(destinationIdRequestBody)
        }.onFailure {
          log.error("Error while listing connections for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
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
  ): DestinationsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery

    val result =
      runCatching {
        destinationHandler.listDestinationsForWorkspaces(listResourcesForWorkspacesRequestBody)
      }.onFailure {
        log.error("Error while listing destinations for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it)
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

  override fun getDestinationSyncModes(destinationId: UUID): List<DestinationSyncMode> {
    val destinationRead: DestinationRead = getDestinationRead(destinationId)
    return getDestinationSyncModes(destinationRead)
  }

  override fun getDestinationSyncModes(destinationRead: DestinationRead): List<DestinationSyncMode> {
    val destinationDefinitionId: UUID = destinationRead.destinationDefinitionId
    val destinationDefinitionIdWithWorkspaceId = DestinationDefinitionIdWithWorkspaceId()
    destinationDefinitionIdWithWorkspaceId.destinationDefinitionId = destinationDefinitionId
    destinationDefinitionIdWithWorkspaceId.workspaceId = destinationRead.workspaceId

    val result =
      kotlin
        .runCatching {
          connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId)
        }.onFailure {
          log.error("Error while listing destinations for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
        }

    val destinationDefinitionSpecificationRead = result.getOrNull()!!
    return destinationDefinitionSpecificationRead.supportedDestinationSyncModes!!
  }
}
