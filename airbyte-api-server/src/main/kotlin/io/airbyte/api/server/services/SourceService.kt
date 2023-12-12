/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.InitiateOauthRequest
import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.airbyte_api.model.generated.SourcesResponse
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.client.model.generated.PartialSourceUpdate
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SourceReadList
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.mappers.SourceReadMapper
import io.airbyte.api.server.mappers.SourcesResponseMapper
import io.airbyte.api.server.problems.UnexpectedProblem
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.client.exceptions.ReadTimeoutException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.ws.rs.core.Response

interface SourceService {
  fun createSource(
    sourceCreateRequest: SourceCreateRequest,
    sourceDefinitionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse

  fun updateSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse

  fun partialUpdateSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse

  fun deleteSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  )

  fun getSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse

  fun getSourceSchema(
    sourceId: UUID,
    disableCache: Boolean,
    authorization: String?,
    userInfo: String?,
  ): SourceDiscoverSchemaRead

  fun listSourcesForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
    authorization: String?,
    userInfo: String?,
  ): SourcesResponse

  fun controllerInitiateOAuth(
    initiateOauthRequest: InitiateOauthRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response
}

@Singleton
@Secondary
open class SourceServiceImpl(
  private val configApiClient: ConfigApiClient,
  private val userService: UserServiceImpl,
) : SourceService {
  companion object {
    private val log = LoggerFactory.getLogger(SourceServiceImpl::class.java)
  }

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  /**
   * Creates a source.
   */
  override fun createSource(
    sourceCreateRequest: SourceCreateRequest,
    sourceDefinitionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse {
    val sourceCreateOss = SourceCreate()
    sourceCreateOss.name = sourceCreateRequest.name
    sourceCreateOss.sourceDefinitionId = sourceDefinitionId
    sourceCreateOss.workspaceId = sourceCreateRequest.workspaceId
    sourceCreateOss.connectionConfiguration = sourceCreateRequest.configuration
    sourceCreateOss.secretId = sourceCreateRequest.secretId

    val response =
      try {
        configApiClient.createSource(sourceCreateOss, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for createSource: ", e)
        e.response as HttpResponse<SourceRead>
      }

    ConfigClientErrorHandler.handleError(response, sourceCreateRequest.workspaceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourceReadMapper.from(response.body()!!)
  }

  /**
   * Updates a source fully with full replacement of configuration.
   */
  override fun updateSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse {
    val sourceUpdate =
      SourceUpdate()
        .sourceId(sourceId)
        .connectionConfiguration(sourcePutRequest.configuration)
        .name(sourcePutRequest.name)

    val response =
      try {
        configApiClient.updateSource(sourceUpdate, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for updateSource: ", e)
        e.response as HttpResponse<SourceRead>
      }

    ConfigClientErrorHandler.handleError(response, sourceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourceReadMapper.from(response.body()!!)
  }

  /**
   * Updates a source allowing patch semantics including within the configuration.
   */
  override fun partialUpdateSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse {
    val sourceUpdate =
      PartialSourceUpdate()
        .sourceId(sourceId)
        .connectionConfiguration(sourcePatchRequest.configuration)
        .name(sourcePatchRequest.name)
        .secretId(sourcePatchRequest.secretId)

    val response =
      try {
        configApiClient.partialUpdateSource(sourceUpdate, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for partialUpdateSource: ", e)
        e.response as HttpResponse<SourceRead>
      }

    ConfigClientErrorHandler.handleError(response, sourceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourceReadMapper.from(response.body()!!)
  }

  /**
   * Deletes a source by ID.
   */
  override fun deleteSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  ) {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceId)
    val response =
      try {
        configApiClient.deleteSource(sourceIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for source delete: ", e)
        e.response as HttpResponse<String>
      }
    ConfigClientErrorHandler.handleError(response, sourceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
  }

  /**
   * Gets a source by ID.
   */
  override fun getSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceResponse {
    val sourceIdRequestBody = SourceIdRequestBody()
    sourceIdRequestBody.sourceId = sourceId
    val response =
      try {
        configApiClient.getSource(sourceIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getSource: ", e)
        e.response as HttpResponse<SourceRead>
      }
    ConfigClientErrorHandler.handleError(response, sourceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourceReadMapper.from(response.body()!!)
  }

  /**
   * Gets a source's schema.
   */
  override fun getSourceSchema(
    sourceId: UUID,
    disableCache: Boolean,
    authorization: String?,
    userInfo: String?,
  ): SourceDiscoverSchemaRead {
    val sourceDiscoverSchemaRequestBody = SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(disableCache)

    val response: HttpResponse<SourceDiscoverSchemaRead> =
      try {
        configApiClient.getSourceSchema(sourceDiscoverSchemaRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getSourceSchema: ", e)
        e.response as HttpResponse<SourceDiscoverSchemaRead>
      } catch (e: ReadTimeoutException) {
        log.error("Config api read timeout error for getSourceSchema: ", e)
        if (disableCache) {
          throw UnexpectedProblem(
            "try-again",
            HttpStatus.REQUEST_TIMEOUT,
            "Updating cache latest source schema in progress. Please try again with cache on.",
          )
        } else {
          throw UnexpectedProblem(HttpStatus.REQUEST_TIMEOUT)
        }
      }
    ConfigClientErrorHandler.handleError(response, sourceId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    if (response.body() == null || response.body()?.jobInfo?.succeeded == false) {
      var errorMessage = "Something went wrong in the connector."
      if (response.body() != null && response.body()?.jobInfo?.failureReason!!.externalMessage != null) {
        errorMessage += " logs:" + response.body()?.jobInfo!!.failureReason!!.externalMessage
      } else if (response.body() != null && response.body()?.jobInfo?.failureReason!!.internalMessage != null) {
        errorMessage += " logs:" + response.body()?.jobInfo!!.failureReason!!.internalMessage
      }
      throw UnexpectedProblem(HttpStatus.BAD_REQUEST, errorMessage)
    }
    return response.body()!!
  }

  /**
   * Lists sources by workspace IDs or all sources if no workspace IDs are provided.
   */
  override fun listSourcesForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String?,
    userInfo: String?,
  ): SourcesResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(userInfo) }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery

    val response =
      try {
        configApiClient.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for listWorkspaces: ", e)
        e.response as HttpResponse<SourceReadList>
      }
    ConfigClientErrorHandler.handleError(response, workspaceIds.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourcesResponseMapper.from(
      response.body()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }

  override fun controllerInitiateOAuth(
    initiateOauthRequest: InitiateOauthRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }
}
