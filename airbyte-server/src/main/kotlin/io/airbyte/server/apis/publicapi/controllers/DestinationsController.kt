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
import io.airbyte.public_api.generated.PublicDestinationsApi
import io.airbyte.public_api.model.generated.DestinationCreateRequest
import io.airbyte.public_api.model.generated.DestinationPatchRequest
import io.airbyte.public_api.model.generated.DestinationPutRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.DESTINATIONS_PATH
import io.airbyte.server.apis.publicapi.constants.DESTINATIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.DESTINATION_TYPE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.PUT
import io.airbyte.server.apis.publicapi.helpers.getActorDefinitionIdFromActorName
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

@Controller(DESTINATIONS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationsController(
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicDestinationsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateDestination(destinationCreateRequest: DestinationCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(destinationCreateRequest.workspaceId.toString()),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val destinationDefinitionId: UUID =
      destinationCreateRequest.definitionId
        ?: run {
          val configurationJsonNode = destinationCreateRequest.configuration as ObjectNode
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
          getActorDefinitionIdFromActorName(DESTINATION_NAME_TO_DEFINITION_ID, destinationName)
        }

    removeDestinationType(destinationCreateRequest)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.createDestination(
            destinationCreateRequest,
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
      destinationCreateRequest.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(destinationResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteDestination(destinationId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.deleteDestination(
            destinationId,
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
  override fun publicGetDestination(destinationId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.getDestination(
            destinationId,
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
    val destinations: Any? =
      trackingHelper.callWithTracker({
        destinationService.listDestinationsForWorkspaces(
          safeWorkspaceIds,
          includeDeleted!!,
          limit!!,
          offset!!,
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

  @Path("/{destinationId}")
  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    removeDestinationType(destinationPatchRequest)

    val destinationResponse: Any =
      trackingHelper.callWithTracker(
        {
          destinationService.partialUpdateDestination(
            destinationId,
            destinationPatchRequest,
          )
        },
        DESTINATIONS_WITH_ID_PATH,
        PATCH,
        userId,
      )!!

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

  @Path("/{destinationId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun putDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    removeDestinationType(destinationPutRequest)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.updateDestination(
            destinationId,
            destinationPutRequest,
          )
        },
        DESTINATIONS_WITH_ID_PATH,
        PUT,
        userId,
      )

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
