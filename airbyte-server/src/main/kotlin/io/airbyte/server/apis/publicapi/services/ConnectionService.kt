/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.publicApi.server.generated.models.ConnectionsResponse
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.helpers.DataResidencyHelper
import io.airbyte.server.apis.publicapi.mappers.ConnectionCreateMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionReadMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionUpdateMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionsResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Collections
import java.util.UUID

interface ConnectionService {
  fun createConnection(
    connectionCreateRequest: ConnectionCreateRequest,
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog,
    workspaceId: UUID,
  ): ConnectionResponse

  fun deleteConnection(connectionId: UUID)

  fun getConnection(connectionId: UUID): ConnectionResponse

  fun updateConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
    workspaceId: UUID,
  ): ConnectionResponse

  fun listConnectionsForWorkspaces(
    workspaceIds: List<UUID> = Collections.emptyList(),
    tagIds: List<UUID> = Collections.emptyList(),
    limit: Int = 20,
    offset: Int = 0,
    includeDeleted: Boolean = false,
  ): ConnectionsResponse
}

@Singleton
@Secondary
class ConnectionServiceImpl(
  private val userService: UserService,
  private val sourceService: SourceService,
  private val connectionHandler: ConnectionsHandler,
  private val currentUserService: CurrentUserService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataResidencyHelper: DataResidencyHelper,
) : ConnectionService {
  companion object {
    private val log = LoggerFactory.getLogger(ConnectionServiceImpl::class.java)
  }

  @Inject
  private lateinit var dataplaneService: DataplaneService

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  /**
   * Creates a connection.
   */
  override fun createConnection(
    connectionCreateRequest: ConnectionCreateRequest,
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog,
    workspaceId: UUID,
  ): ConnectionResponse {
    val connectionCreateOss: ConnectionCreate =
      ConnectionCreateMapper.from(
        connectionCreateRequest,
        catalogId,
        configuredCatalog,
        dataResidencyHelper.getDataplaneGroupFromResidencyAndAirbyteEdition(connectionCreateRequest.dataResidency)?.id,
      )

    val result: Result<ConnectionRead> =
      kotlin
        .runCatching { connectionHandler.createConnection(connectionCreateOss) }
        .onFailure {
          log.error("Error while creating connection: ", it)
          throw ConfigClientErrorHandler.handleCreateConnectionError(it, connectionCreateRequest)
        }.onSuccess { log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + it) }

    return try {
      ConnectionReadMapper.from(
        result.getOrNull()!!,
        workspaceId,
        dataplaneGroupService.getDataplaneGroup(result.getOrNull()!!.dataplaneGroupId).name,
      )
    } catch (e: Exception) {
      log.error("Error while reading response and converting to Connection read: ", e)
      throw UnexpectedProblem()
    }
  }

  /**
   * Deletes a connection by ID.
   */
  override fun deleteConnection(connectionId: UUID) {
    val result =
      kotlin
        .runCatching {
          connectionHandler.deleteConnection(connectionId)
        }.onFailure {
          log.error("Error while deleting connection: ", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result.getOrNull())
  }

  /**
   * Gets a connection by ID.
   */
  override fun getConnection(connectionId: UUID): ConnectionResponse {
    val result =
      runCatching {
        connectionHandler.getConnection(connectionId)
      }.onFailure {
        log.error("Error while getting connection: ", it)
        ConfigClientErrorHandler.handleError(it)
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)

    val connectionRead = result.getOrNull()!!

    // get workspace id from source id
    val sourceResponse: SourceResponse = sourceService.getSource(connectionRead.sourceId, false)

    return ConnectionReadMapper.from(
      connectionRead,
      UUID.fromString(sourceResponse.workspaceId),
      dataplaneGroupService.getDataplaneGroup(connectionRead.dataplaneGroupId).name,
    )
  }

  /**
   * Updates a connection with patch semantics.
   */
  override fun updateConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
    workspaceId: UUID,
  ): ConnectionResponse {
    val dataplaneGroupId: UUID? =
      if (connectionPatchRequest.dataResidency != null) {
        dataResidencyHelper.getDataplaneGroupFromResidencyAndAirbyteEdition(connectionPatchRequest.dataResidency)?.id
      } else {
        null
      }

    val connectionUpdate: ConnectionUpdate =
      ConnectionUpdateMapper.from(
        connectionId,
        connectionPatchRequest,
        catalogId,
        configuredCatalog,
        dataplaneGroupId,
      )

    // this is kept as a string to easily parse the error response to determine if a source or a
    // destination id is invalid
    val result =
      kotlin
        .runCatching { connectionHandler.updateConnection(connectionUpdate, null, false) }
        .onFailure {
          log.error("Error while updating connection: ", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)

    val connectionRead = result.getOrNull()!!

    return try {
      ConnectionReadMapper.from(
        connectionRead,
        workspaceId,
        dataplaneGroupService.getDataplaneGroup(connectionRead.dataplaneGroupId).name,
      )
    } catch (e: java.lang.Exception) {
      log.error("Error while reading and converting to Connection Response: ", e)
      throw UnexpectedProblem()
    }
  }

  /**
   * Lists connections for a set of workspace IDs or all workspaces if none are provided.
   */
  override fun listConnectionsForWorkspaces(
    workspaceIds: List<UUID>,
    tagIds: List<UUID>,
    limit: Int,
    offset: Int,
    includeDeleted: Boolean,
  ): ConnectionsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }

    val listConnectionsForWorkspacesRequestBody =
      ListConnectionsForWorkspacesRequestBody()
        .workspaceIds(workspaceIdsToQuery)
        .tagIds(tagIds)
        .includeDeleted(includeDeleted)
        .pagination(pagination)

    val result =
      kotlin
        .runCatching {
          connectionHandler.listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody)
        }.onFailure {
          log.error("Error while listing connections for workspaces: ", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val connectionReadList = result.getOrNull()!!

    return ConnectionsResponseMapper.from(
      connectionReadList,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
      connectionReadList.connections.map {
        dataplaneGroupService.getDataplaneGroup(it.dataplaneGroupId).name
      },
    )
  }
}
