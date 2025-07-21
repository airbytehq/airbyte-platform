/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.problems.model.generated.ProblemValueData
import io.airbyte.api.problems.throwable.generated.UnknownValueProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicSourcesApi
import io.airbyte.publicApi.server.generated.models.InitiateOauthRequest
import io.airbyte.publicApi.server.generated.models.SourceCreateRequest
import io.airbyte.publicApi.server.generated.models.SourcePatchRequest
import io.airbyte.publicApi.server.generated.models.SourcePutRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.PUT
import io.airbyte.server.apis.publicapi.constants.SOURCES_PATH
import io.airbyte.server.apis.publicapi.constants.SOURCES_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.SOURCE_TYPE
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
@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourcesController(
  private val sourceService: SourceService,
  private val trackingHelper: TrackingHelper,
  private val currentUserService: CurrentUserService,
  private val roleResolver: RoleResolver,
) : PublicSourcesApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.EMBEDDED_END_USER)
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateSource(sourceCreateRequest: SourceCreateRequest?): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val sourceDefinitionId: UUID =
      sourceCreateRequest?.definitionId
        ?: run {
          val configurationJsonNode = sourceCreateRequest?.configuration as ObjectNode
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
          SOURCE_NAME_TO_DEFINITION_ID[sourceName] ?: throw UnknownValueProblem(
            ProblemValueData().value(sourceName),
          )
        }

    val sourceResponse =
      sourceCreateRequest?.let { request ->
        removeSourceTypeNode(request)
        val response =
          trackingHelper.callWithTracker(
            {
              sourceService.createSource(
                request,
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
          request.workspaceId,
        )

        response
      }

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(sourceResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteSource(sourceId: String): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.SOURCE_ID, sourceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.deleteSource(
            UUID.fromString(sourceId),
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
  override fun publicGetSource(
    sourceId: String,
    includeSecretCoordinates: Boolean?,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.SOURCE_ID, sourceId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)

    val sourceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          sourceService.getSource(
            UUID.fromString(sourceId),
            includeSecretCoordinates,
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

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun initiateOAuth(initiateOauthRequest: InitiateOauthRequest): Response = sourceService.controllerInitiateOAuth(initiateOauthRequest)

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun listSources(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    // If workspace IDs were given, then verify the user has access to those workspaces.
    // If none were given, then the SourceService determine the workspaces for the current user.
    if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val sources: Any? =
      trackingHelper.callWithTracker({
        sourceService.listSourcesForWorkspaces(
          safeWorkspaceIds,
          includeDeleted,
          limit,
          offset,
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
  @Path("$SOURCES_PATH/{sourceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchSource(
    sourceId: String,
    sourcePatchRequest: SourcePatchRequest?,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.SOURCE_ID, sourceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val sourceResponse: Any? =
      sourcePatchRequest?.let { request ->
        removeSourceTypeNode(request)

        trackingHelper.callWithTracker(
          {
            sourceService.partialUpdateSource(
              UUID.fromString(sourceId),
              request,
            )
          },
          SOURCES_WITH_ID_PATH,
          PATCH,
          userId,
        )
      }

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

  @Path("$SOURCES_PATH/{sourceId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun putSource(
    sourceId: String,
    sourcePutRequest: SourcePutRequest?,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.SOURCE_ID, sourceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val sourceResponse: Any? =
      sourcePutRequest?.let { request ->
        removeSourceTypeNode(request)

        trackingHelper.callWithTracker(
          {
            sourceService.updateSource(
              UUID.fromString(sourceId),
              request,
            )
          },
          SOURCES_WITH_ID_PATH,
          PUT,
          userId,
        )
      }

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
