/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.errors.problems.UnprocessableEntityProblem
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicSourcesApi
import io.airbyte.public_api.model.generated.InitiateOauthRequest
import io.airbyte.public_api.model.generated.SourceCreateRequest
import io.airbyte.public_api.model.generated.SourcePatchRequest
import io.airbyte.public_api.model.generated.SourcePutRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.PUT
import io.airbyte.server.apis.publicapi.constants.SOURCES_PATH
import io.airbyte.server.apis.publicapi.constants.SOURCES_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.SOURCE_TYPE
import io.airbyte.server.apis.publicapi.helpers.getActorDefinitionIdFromActorName
import io.airbyte.server.apis.publicapi.helpers.removeSourceTypeNode
import io.airbyte.server.apis.publicapi.mappers.SOURCE_NAME_TO_DEFINITION_ID
import io.airbyte.server.apis.publicapi.services.SourceService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

// Marked as open because when not marked, micronaut failed to start up because generated beans couldn't extend this one since it was "final"
@Controller(SOURCES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourcesController(
  private val sourceService: SourceService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicSourcesApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateSource(sourceCreateRequest: SourceCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(sourceCreateRequest.workspaceId.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val sourceDefinitionId: UUID =
      sourceCreateRequest.definitionId
        ?: run {
          val configurationJsonNode = sourceCreateRequest.configuration as ObjectNode
          if (configurationJsonNode.findValue(SOURCE_TYPE) == null) {
            val unprocessableEntityProblem = UnprocessableEntityProblem()
            trackingHelper.trackFailuresIfAny(
              SOURCES_PATH,
              POST,
              userId,
              unprocessableEntityProblem,
            )
            throw unprocessableEntityProblem
          }
          val sourceName = configurationJsonNode.findValue(SOURCE_TYPE).toString().replace("\"", "")
          getActorDefinitionIdFromActorName(SOURCE_NAME_TO_DEFINITION_ID, sourceName)
        }

    removeSourceTypeNode(sourceCreateRequest)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.createSource(
            sourceCreateRequest,
            sourceDefinitionId,
          )
        },
        SOURCES_PATH,
        POST,
        userId,
      )

    trackingHelper.trackSuccess(
      SOURCES_PATH,
      POST,
      userId,
      sourceCreateRequest.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteSource(sourceId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(sourceId.toString()),
      Scope.SOURCE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.deleteSource(
            sourceId,
          )
        },
        SOURCES_WITH_ID_PATH,
        DELETE,
        userId,
      )

    trackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(sourceResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetSource(sourceId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(sourceId.toString()),
      Scope.SOURCE,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.getSource(
            sourceId,
          )
        },
        SOURCES_WITH_ID_PATH,
        GET,
        userId,
      )

    trackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun initiateOAuth(initiateOauthRequest: InitiateOauthRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(initiateOauthRequest.workspaceId.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return sourceService.controllerInitiateOAuth(initiateOauthRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun listSources(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      workspaceIds?.map { it.toString() } ?: emptyList(),
      Scope.WORKSPACES,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val sources: Any? =
      trackingHelper.callWithTracker({
        sourceService.listSourcesForWorkspaces(
          safeWorkspaceIds,
          includeDeleted!!,
          limit!!,
          offset!!,
        )
      }, SOURCES_PATH, GET, userId)

    trackingHelper.trackSuccess(
      SOURCES_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sources)
      .build()
  }

  @Patch
  @Path("/{sourceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(sourceId.toString()),
      Scope.SOURCE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    removeSourceTypeNode(sourcePatchRequest)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.partialUpdateSource(
            sourceId,
            sourcePatchRequest,
          )
        },
        SOURCES_WITH_ID_PATH,
        PATCH,
        userId,
      )

    trackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      PATCH,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  @Path("/{sourceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun putSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(sourceId.toString()),
      Scope.SOURCE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    removeSourceTypeNode(sourcePutRequest)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.updateSource(
            sourceId,
            sourcePutRequest,
          )
        },
        SOURCES_WITH_ID_PATH,
        PUT,
        userId,
      )

    trackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      PUT,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }
}
