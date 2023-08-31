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
import io.airbyte.api.server.helpers.getIdFromName
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

@Controller(SOURCES_PATH)
class SourcesController(
  private val sourceService: SourceService,
  private val userService: UserService,
) : SourcesApi {
  override fun createSource(sourceCreateRequest: SourceCreateRequest?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val configurationJsonNode = sourceCreateRequest!!.configuration as ObjectNode
    if (configurationJsonNode.findValue(SOURCE_TYPE) == null) {
      throw UnprocessableEntityProblem()
    }
    val sourceName = configurationJsonNode.findValue(SOURCE_TYPE).toString().replace("\"", "")
    val sourceDefinitionId: UUID = getIdFromName(SOURCE_NAME_TO_DEFINITION_ID, sourceName)

    removeSourceTypeNode(sourceCreateRequest)

    val sourceResponse: Any? = TrackingHelper.callWithTracker(
      {
        sourceService.createSource(
          sourceCreateRequest,
          sourceDefinitionId,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      SOURCES_PATH,
      POST,
      userId,
    )

    TrackingHelper.trackSuccess(
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

  override fun deleteSource(sourceId: UUID?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val sourceResponse: Any? = TrackingHelper.callWithTracker(
      {
        sourceService.deleteSource(
          sourceId!!,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      SOURCES_WITH_ID_PATH,
      DELETE,
      userId,
    )

    TrackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(sourceResponse)
      .build()
  }

  override fun getSource(sourceId: UUID?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val sourceResponse: Any? = TrackingHelper.callWithTracker(
      {
        sourceService.getSource(
          sourceId!!,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      SOURCES_WITH_ID_PATH,
      GET,
      userId,
    )

    TrackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  override fun initiateOAuth(initiateOauthRequest: InitiateOauthRequest?, userInfo: String?): Response {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build()
  }

  override fun listSources(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,

    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val sources: Any? = TrackingHelper.callWithTracker({
      sourceService.listSourcesForWorkspaces(
        safeWorkspaceIds,
        includeDeleted!!,
        limit!!,
        offset!!,
        getLocalUserInfoIfNull(userInfo),
      )
    }, SOURCES_PATH, GET, userId)

    TrackingHelper.trackSuccess(
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
  override fun patchSource(sourceId: UUID?, sourcePatchRequest: SourcePatchRequest?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeSourceTypeNode(sourcePatchRequest!!)

    val sourceResponse: Any? = TrackingHelper.callWithTracker(
      {
        sourceService.partialUpdateSource(
          sourceId!!,
          sourcePatchRequest,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      SOURCES_WITH_ID_PATH,
      PATCH,
      userId,
    )

    TrackingHelper.trackSuccess(
      SOURCES_WITH_ID_PATH,
      PATCH,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  override fun putSource(sourceId: UUID?, sourcePutRequest: SourcePutRequest?, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeSourceTypeNode(sourcePutRequest!!)

    val sourceResponse: Any? = TrackingHelper.callWithTracker(
      {
        sourceService.updateSource(
          sourceId!!,
          sourcePutRequest,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      SOURCES_WITH_ID_PATH,
      PUT,
      userId,
    )

    TrackingHelper.trackSuccess(
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
