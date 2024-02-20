/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package controllers

import authorization.AirbyteApiAuthorizationHelper
import authorization.Scope
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.airbyte_api.generated.SourcesApi
import io.airbyte.airbyte_api.model.generated.InitiateOauthRequest
import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.api.model.generated.PermissionType
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
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID
import javax.ws.rs.Path
import javax.ws.rs.core.Response

// Marked as open because when not marked, micronaut failed to start up because generated beans couldn't extend this one since it was "final"
@Controller(SOURCES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourcesController(
  private val sourceService: SourceService,
  private val trackingHelper: TrackingHelper,
  private val airbyteApiAuthorizationHelper: AirbyteApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : SourcesApi {
  override fun createSource(
    sourceCreateRequest: SourceCreateRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(initiateOauthRequest.workspaceId.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    return sourceService.controllerInitiateOAuth(initiateOauthRequest, userInfo)
  }

  override fun listSources(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      workspaceIds?.map { toString() } ?: emptyList(),
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
  @Path("/{sourceId}")
  override fun patchSource(
    sourceId: UUID,
    sourcePatchRequest: SourcePatchRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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

  @Path("/{sourceId}")
  override fun putSource(
    sourceId: UUID,
    sourcePutRequest: SourcePutRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
