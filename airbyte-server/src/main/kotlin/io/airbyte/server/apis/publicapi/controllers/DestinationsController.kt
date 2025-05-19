/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.problems.model.generated.ProblemValueData
import io.airbyte.api.problems.throwable.generated.UnknownValueProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicDestinationsApi
import io.airbyte.publicApi.server.generated.models.DestinationCreateRequest
import io.airbyte.publicApi.server.generated.models.DestinationPatchRequest
import io.airbyte.publicApi.server.generated.models.DestinationPutRequest
import io.airbyte.publicApi.server.generated.models.DestinationResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.DESTINATIONS_PATH
import io.airbyte.server.apis.publicapi.constants.DESTINATIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.DESTINATION_TYPE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.PUT
import io.airbyte.server.apis.publicapi.helpers.removeDestinationType
import io.airbyte.server.apis.publicapi.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationsController(
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
) : PublicDestinationsApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateDestination(destinationCreateRequest: DestinationCreateRequest?): Response {
    val destinationResponse: Any? =
      destinationCreateRequest?.let { request ->
        val userId: UUID = currentUserService.currentUser.userId

        val destinationDefinitionId: UUID =
          request.definitionId
            ?: run {
              val configurationJsonNode = request.configuration as ObjectNode
              if (configurationJsonNode.findValue(DESTINATION_TYPE) == null) {
                val unprocessableEntityProblem = UnprocessableEntityProblem()
                trackingHelper.trackFailuresIfAny(
                  DESTINATIONS_PATH,
                  POST,
                  userId,
                  unprocessableEntityProblem,
                )
                throw unprocessableEntityProblem
              }
              val destinationName = configurationJsonNode.findValue(DESTINATION_TYPE).toString().replace("\"", "")
              DESTINATION_NAME_TO_DEFINITION_ID[destinationName] ?: throw UnknownValueProblem(
                ProblemValueData().value(destinationName),
              )
            }

        removeDestinationType(request)

        val response =
          trackingHelper.callWithTracker(
            {
              destinationService.createDestination(
                request,
                destinationDefinitionId,
              )
            },
            DESTINATIONS_PATH,
            POST,
            userId,
          )

        trackingHelper.trackSuccess(
          DESTINATIONS_PATH,
          POST,
          userId,
          request.workspaceId,
        )

        response
      }

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinationResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteDestination(destinationId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.DESTINATION_ID_, destinationId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.deleteDestination(
            UUID.fromString(destinationId),
          )
        },
        DESTINATIONS_WITH_ID_PATH,
        DELETE,
        userId,
      )
    trackingHelper.trackSuccess(
      DESTINATIONS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(destinationResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetDestination(
    destinationId: String,
    includeSecretCoordinates: Boolean?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.DESTINATION_ID_, destinationId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.getDestination(
            UUID.fromString(destinationId),
            includeSecretCoordinates,
          )
        },
        DESTINATIONS_WITH_ID_PATH,
        GET,
        userId,
      )
    trackingHelper.trackSuccess(
      DESTINATIONS_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinationResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun listDestinations(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    // If workspace IDs were given, then verify the user has access to those workspaces.
    // If none were given, then the DestinationService will determine the workspaces for the current user.
    if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .Request()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val destinations: Any? =
      trackingHelper.callWithTracker({
        destinationService.listDestinationsForWorkspaces(
          safeWorkspaceIds,
          includeDeleted,
          limit,
          offset,
        )
      }, DESTINATIONS_PATH, GET, userId)
    trackingHelper.trackSuccess(
      DESTINATIONS_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinations)
      .build()
  }

  @Path("$DESTINATIONS_PATH/{destinationId}")
  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchDestination(
    destinationId: String,
    destinationPatchRequest: DestinationPatchRequest?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.DESTINATION_ID_, destinationId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val destinationResponse: DestinationResponse? =
      destinationPatchRequest?.let { patch ->
        removeDestinationType(patch)
        trackingHelper.callWithTracker(
          {
            destinationService.partialUpdateDestination(UUID.fromString(destinationId), patch)
          },
          DESTINATIONS_WITH_ID_PATH,
          PATCH,
          userId,
        )
      }
    trackingHelper.trackSuccess(
      DESTINATIONS_WITH_ID_PATH,
      PATCH,
      userId,
    )

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinationResponse)
      .build()
  }

  @Path("$DESTINATIONS_PATH/{destinationId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun putDestination(
    destinationId: String,
    destinationPutRequest: DestinationPutRequest?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.DESTINATION_ID_, destinationId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val destinationResponse: Any? =
      destinationPutRequest?.let { request ->
        removeDestinationType(request)
        trackingHelper.callWithTracker(
          {
            destinationService.updateDestination(
              UUID.fromString(destinationId),
              destinationPutRequest,
            )
          },
          DESTINATIONS_WITH_ID_PATH,
          PUT,
          userId,
        )
      }

    trackingHelper.trackSuccess(
      DESTINATIONS_WITH_ID_PATH,
      PUT,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinationResponse)
      .build()
  }
}
