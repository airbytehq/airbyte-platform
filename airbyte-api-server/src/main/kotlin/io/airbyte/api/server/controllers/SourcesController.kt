/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.airbyte_api.generated.SourcesApi
import io.airbyte.airbyte_api.model.generated.InitiateOauthRequest
import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.PATCH
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.PUT
import io.airbyte.api.server.constants.SOURCES_PATH
import io.airbyte.api.server.constants.SOURCES_WITH_ID_PATH
import io.airbyte.api.server.constants.SOURCE_TYPE
import io.airbyte.api.server.helpers.getActorDefinitionIdFromActorName
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.helpers.removeSourceTypeNode
import io.airbyte.api.server.mappers.SOURCE_NAME_TO_DEFINITION_ID
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.api.server.services.SourceService
import io.airbyte.api.server.services.UserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import java.util.UUID
import javax.ws.rs.core.Response

// Marked as open because when not marked, micronaut failed to start up because generated beans couldn't extend this one since it was "final"
@Controller(SOURCES_PATH)
open class SourcesController(
  private val sourceService: SourceService,
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
) : SourcesApi {
  override fun createSource(
    sourceCreateRequest: SourceCreateRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

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
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun deleteSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.deleteSource(
            sourceId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun getSource(
    sourceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.getSource(
            sourceId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun initiateOAuth(
    initiateOauthRequest: InitiateOauthRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    return sourceService.controllerInitiateOAuth(initiateOauthRequest, authorization, userInfo)
  }

  override fun listSources(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val sources: Any? =
      trackingHelper.callWithTracker({
        sourceService.listSourcesForWorkspaces(
          safeWorkspaceIds,
          includeDeleted!!,
          limit!!,
          offset!!,
          authorization,
          getLocalUserInfoIfNull(userInfo),
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
  override fun patchSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeSourceTypeNode(sourcePatchRequest)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.partialUpdateSource(
            sourceId,
            sourcePatchRequest,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun putSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeSourceTypeNode(sourcePutRequest)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.updateSource(
            sourceId,
            sourcePutRequest,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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
