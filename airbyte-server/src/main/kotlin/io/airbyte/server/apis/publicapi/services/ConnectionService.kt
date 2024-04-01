/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.commons.server.errors.problems.UnexpectedProblem
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.model.generated.ConnectionCreateRequest
import io.airbyte.public_api.model.generated.ConnectionPatchRequest
import io.airbyte.public_api.model.generated.ConnectionResponse
import io.airbyte.public_api.model.generated.ConnectionsResponse
import io.airbyte.public_api.model.generated.SourceResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.ConnectionCreateMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionReadMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionUpdateMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionsResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpStatus
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
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog?,
    workspaceId: UUID,
  ): ConnectionResponse

  fun listConnectionsForWorkspaces(
    workspaceIds: List<UUID> = Collections.emptyList(),
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
) : ConnectionService {
  companion object {
    private val log = LoggerFactory.getLogger(ConnectionServiceImpl::class.java)
  }

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
      ConnectionCreateMapper.from(connectionCreateRequest, catalogId, configuredCatalog)

    val result: Result<ConnectionRead> =
      kotlin.runCatching { connectionHandler.createConnection(connectionCreateOss) }
        .onFailure {
          log.error("Error while creating connection: ", it)
          throw ConfigClientErrorHandler.handleCreateConnectionError(it, connectionCreateRequest)
        }
        .onSuccess { log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + it) }

    return try {
      ConnectionReadMapper.from(
        result.getOrNull()!!,
        workspaceId,
      )
    } catch (e: Exception) {
      log.error("Error while reading response and converting to Connection read: ", e)
      throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }

  /**
   * Deletes a connection by ID.
   */
  override fun deleteConnection(connectionId: UUID) {
    val result =
      kotlin.runCatching {
        connectionHandler.deleteConnection(connectionId)
      }.onFailure {
        log.error("Error while deleting connection: ", it)
        ConfigClientErrorHandler.handleError(it, connectionId.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result.getOrNull())
  }

  /**
   * Gets a connection by ID.
   */
  override fun getConnection(connectionId: UUID): ConnectionResponse {
    val result =
      kotlin.runCatching {
        connectionHandler.getConnection(connectionId)
      }.onFailure {
        log.error("Error while getting connection: ", it)
        ConfigClientErrorHandler.handleError(it, connectionId.toString())
      }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)

    val connectionRead = result.getOrNull()!!

    // get workspace id from source id
    val sourceResponse: SourceResponse = sourceService.getSource(connectionRead.sourceId)

    return ConnectionReadMapper.from(
      connectionRead,
      sourceResponse.workspaceId,
    )
  }

  /**
   * Updates a connection with patch semantics.
   */
  override fun updateConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog?,
    workspaceId: UUID,
  ): ConnectionResponse {
    val connectionUpdate: ConnectionUpdate =
      ConnectionUpdateMapper.from(
        connectionId,
        connectionPatchRequest,
        catalogId,
        configuredCatalog,
      )

    // this is kept as a string to easily parse the error response to determine if a source or a
    // destination id is invalid
    val result =
      kotlin.runCatching { connectionHandler.updateConnection(connectionUpdate) }
        .onFailure {
          log.error("Error while updating connection: ", it)
          ConfigClientErrorHandler.handleError(it, connectionId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)

    val connectionRead = result.getOrNull()!!

    return try {
      ConnectionReadMapper.from(
        connectionRead,
        workspaceId,
      )
    } catch (e: java.lang.Exception) {
      log.error("Error while reading and converting to Connection Response: ", e)
      throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }

  /**
   * Lists connections for a set of workspace IDs or all workspaces if none are provided.
   */
  override fun listConnectionsForWorkspaces(
    workspaceIds: List<UUID>,
    limit: Int,
    offset: Int,
    includeDeleted: Boolean,
  ): ConnectionsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }

    val listConnectionsForWorkspacesRequestBody =
      ListConnectionsForWorkspacesRequestBody()
        .workspaceIds(workspaceIdsToQuery)
        .includeDeleted(includeDeleted)
        .pagination(pagination)

    val result =
      kotlin.runCatching {
        connectionHandler.listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody)
      }.onFailure {
        log.error("Error while listing connections for workspaces: ", it)
        ConfigClientErrorHandler.handleError(it, workspaceIds.toString())
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
    )
  }
}
