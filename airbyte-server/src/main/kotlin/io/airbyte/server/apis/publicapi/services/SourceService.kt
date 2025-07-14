/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.models.InitiateOauthRequest
import io.airbyte.publicApi.server.generated.models.SourceCreateRequest
import io.airbyte.publicApi.server.generated.models.SourcePatchRequest
import io.airbyte.publicApi.server.generated.models.SourcePutRequest
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.airbyte.publicApi.server.generated.models.SourcesResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.helpers.toInternal
import io.airbyte.server.apis.publicapi.mappers.SourceReadMapper
import io.airbyte.server.apis.publicapi.mappers.SourcesResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.util.UUID

interface SourceService {
  fun createSource(
    sourceCreateRequest: SourceCreateRequest,
    sourceDefinitionId: UUID,
  ): SourceResponse

  fun updateSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
  ): SourceResponse

  fun partialUpdateSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
  ): SourceResponse

  fun deleteSource(sourceId: UUID)

  fun getSource(
    sourceId: UUID,
    includeSecretCoordinates: Boolean?,
  ): SourceResponse

  fun getSourceSchema(
    sourceId: UUID,
    disableCache: Boolean,
  ): SourceDiscoverSchemaRead

  fun listSourcesForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): SourcesResponse

  fun controllerInitiateOAuth(initiateOauthRequest: InitiateOauthRequest?): Response
}

@Singleton
@Secondary
open class SourceServiceImpl(
  private val userService: UserServiceImpl,
  private val sourceHandler: SourceHandler,
  private val schedulerHandler: SchedulerHandler,
  private val currentUserService: CurrentUserService,
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
  ): SourceResponse {
    val sourceCreateOss = SourceCreate()
    sourceCreateOss.name = sourceCreateRequest.name
    sourceCreateOss.sourceDefinitionId = sourceDefinitionId
    sourceCreateOss.workspaceId = sourceCreateRequest.workspaceId
    sourceCreateOss.connectionConfiguration = sourceCreateRequest.configuration
    sourceCreateOss.secretId = sourceCreateRequest.secretId
    sourceCreateOss.resourceAllocation = sourceCreateRequest.resourceAllocation?.toInternal()

    val result =
      kotlin
        .runCatching { sourceHandler.createSourceWithOptionalSecret(sourceCreateOss) }
        .onFailure {
          log.error("Error for createSource", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceReadMapper.from(result.getOrNull()!!)
  }

  /**
   * Updates a source fully with full replacement of configuration.
   */
  override fun updateSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
  ): SourceResponse {
    val sourceUpdate =
      SourceUpdate()
        .sourceId(sourceId)
        .connectionConfiguration(sourcePutRequest.configuration)
        .name(sourcePutRequest.name)
        .resourceAllocation(sourcePutRequest.resourceAllocation?.toInternal())

    val result =
      kotlin
        .runCatching { sourceHandler.updateSource(sourceUpdate) }
        .onFailure {
          log.error("Error for updateSource", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceReadMapper.from(result.getOrNull()!!)
  }

  /**
   * Updates a source allowing patch semantics including within the configuration.
   */
  override fun partialUpdateSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
  ): SourceResponse {
    val sourceUpdate =
      PartialSourceUpdate()
        .sourceId(sourceId)
        .connectionConfiguration(sourcePatchRequest.configuration)
        .name(sourcePatchRequest.name)
        .secretId(sourcePatchRequest.secretId)
        .resourceAllocation(sourcePatchRequest.resourceAllocation?.toInternal())

    val result =
      kotlin
        .runCatching { sourceHandler.partialUpdateSource(sourceUpdate) }
        .onFailure {
          log.error("Error for partialUpdateSource", it)
          ConfigClientErrorHandler.handleError(it)
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceReadMapper.from(result.getOrNull()!!)
  }

  /**
   * Deletes a source by ID.
   */
  override fun deleteSource(sourceId: UUID) {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceId)
    val result =
      kotlin
        .runCatching { sourceHandler.deleteSource(sourceIdRequestBody) }
        .onFailure {
          log.error("Error for deleteSource", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
  }

  /**
   * Gets a source by ID.
   */
  override fun getSource(
    sourceId: UUID,
    includeSecretCoordinates: Boolean?,
  ): SourceResponse {
    val sourceIdRequestBody = SourceIdRequestBody()
    sourceIdRequestBody.sourceId = sourceId

    val result =
      kotlin
        .runCatching { sourceHandler.getSource(sourceIdRequestBody, includeSecretCoordinates == true) }
        .onFailure {
          log.error("Error for getSource", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceReadMapper.from(result.getOrNull()!!)
  }

  /**
   * Gets a source's schema.
   */
  override fun getSourceSchema(
    sourceId: UUID,
    disableCache: Boolean,
  ): SourceDiscoverSchemaRead {
    val sourceDiscoverSchemaRequestBody = SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(disableCache)

    val result =
      kotlin
        .runCatching { schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchemaRequestBody) }
        .onFailure {
          log.error("Error for getSourceSchema", it)
          ConfigClientErrorHandler.handleError(it)
        }

    val sourceDefinitionSpecificationRead = result.getOrNull()!!
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    if (sourceDefinitionSpecificationRead.jobInfo?.succeeded == false) {
      var errorMessage = "Something went wrong in the connector."
      if (sourceDefinitionSpecificationRead.jobInfo?.failureReason!!.externalMessage != null) {
        errorMessage += " logs:" + sourceDefinitionSpecificationRead.jobInfo!!.failureReason!!.externalMessage
      } else if (sourceDefinitionSpecificationRead.jobInfo?.failureReason!!.internalMessage != null) {
        errorMessage += " logs:" + sourceDefinitionSpecificationRead.jobInfo!!.failureReason!!.internalMessage
      }
      throw UnexpectedProblem(ProblemMessageData().message(errorMessage))
    }
    return result.getOrNull()!!
  }

  /**
   * Lists sources by workspace IDs or all sources if no workspace IDs are provided.
   */
  override fun listSourcesForWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): SourcesResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery

    val result =
      kotlin
        .runCatching { sourceHandler.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody) }
        .onFailure {
          log.error("Error for listSourcesForWorkspaces", it)
          ConfigClientErrorHandler.handleError(it)
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourcesResponseMapper.from(
      result.getOrNull()!!,
      workspaceIds,
      includeDeleted,
      limit,
      offset,
      publicApiHost!!,
    )
  }

  override fun controllerInitiateOAuth(initiateOauthRequest: InitiateOauthRequest?): Response =
    Response.status(Response.Status.NOT_IMPLEMENTED).build()
}
