/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionReadList
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.mappers.ConnectionCreateMapper
import io.airbyte.api.server.mappers.ConnectionReadMapper
import io.airbyte.api.server.mappers.ConnectionUpdateMapper
import io.airbyte.api.server.mappers.ConnectionsResponseMapper
import io.airbyte.api.server.problems.UnexpectedProblem
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.exceptions.HttpClientResponseException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Collections
import java.util.Objects
import java.util.UUID

interface ConnectionService {
  fun createConnection(
    connectionCreateRequest: ConnectionCreateRequest,
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog,
    workspaceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): ConnectionResponse

  fun deleteConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  )

  fun getConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): ConnectionResponse

  fun updateConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID,
    configuredCatalog: AirbyteCatalog?,
    workspaceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): ConnectionResponse

  fun listConnectionsForWorkspaces(
    workspaceIds: List<UUID> = Collections.emptyList(),
    limit: Int = 20,
    offset: Int = 0,
    includeDeleted: Boolean = false,
    authorization: String?,
    userInfo: String?,
  ): ConnectionsResponse
}

@Singleton
@Secondary
class ConnectionServiceImpl(
  private val configApiClient: ConfigApiClient,
  private val userService: UserService,
  private val sourceService: SourceService,
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
    authorization: String?,
    userInfo: String?,
  ): ConnectionResponse {
    val connectionCreateOss: ConnectionCreate =
      ConnectionCreateMapper.from(connectionCreateRequest, catalogId, configuredCatalog)

    val response =
      try {
        configApiClient.createConnection(connectionCreateOss, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for createConnection: ", e)
        // this is kept as a string to easily parse the error response to determine if a source or a
        // destination id is invalid
        e.response as HttpResponse<String>
      }

    ConfigClientErrorHandler.handleCreateConnectionError(response, connectionCreateRequest)
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())

    val objectMapper = ObjectMapper()
    return try {
      ConnectionReadMapper.from(
        objectMapper.readValue(
          Objects.requireNonNull(response.body()),
          ConnectionRead::class.java,
        ),
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
  override fun deleteConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ) {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val response =
      try {
        configApiClient.deleteConnection(connectionIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for connection delete: ", e)
        e.response as HttpResponse<String>
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
  }

  /**
   * Gets a connection by ID.
   */
  override fun getConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): ConnectionResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody()
    connectionIdRequestBody.connectionId = connectionId

    val response =
      try {
        configApiClient.getConnection(connectionIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getConnection: ", e)
        e.response as HttpResponse<ConnectionRead>
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())

    // get workspace id from source id
    val sourceResponse: SourceResponse = sourceService.getSource(response.body()!!.sourceId, authorization, userInfo)

    return ConnectionReadMapper.from(
      response.body()!!,
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
    authorization: String?,
    userInfo: String?,
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
    val response =
      try {
        configApiClient.updateConnection(connectionUpdate, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for updateConnection: ", e)
        e.response as HttpResponse<String>
      }

    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())

    val objectMapper = ObjectMapper()
    return try {
      ConnectionReadMapper.from(
        objectMapper.readValue(
          Objects.requireNonNull(response.body()),
          ConnectionRead::class.java,
        ),
        workspaceId,
      )
    } catch (e: java.lang.Exception) {
      log.error("Error while reading response and converting to Connection read: ", e)
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
    authorization: String?,
    userInfo: String?,
  ): ConnectionsResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(userInfo) }

    val listConnectionsForWorkspacesRequestBody =
      ListConnectionsForWorkspacesRequestBody()
        .workspaceIds(workspaceIdsToQuery)
        .includeDeleted(includeDeleted)
        .pagination(pagination)

    val response =
      try {
        configApiClient.listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for listConnectionsForWorkspaces: ", e)
        e.response as HttpResponse<ConnectionReadList>
      }
    ConfigClientErrorHandler.handleError(response, workspaceIds.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return ConnectionsResponseMapper.from(
      response.body()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }
}
