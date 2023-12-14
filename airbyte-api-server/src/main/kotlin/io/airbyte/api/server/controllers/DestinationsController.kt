/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.airbyte_api.generated.DestinationsApi
import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.DESTINATIONS_PATH
import io.airbyte.api.server.constants.DESTINATIONS_WITH_ID_PATH
import io.airbyte.api.server.constants.DESTINATION_TYPE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.PATCH
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.PUT
import io.airbyte.api.server.helpers.getActorDefinitionIdFromActorName
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.helpers.removeDestinationType
import io.airbyte.api.server.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.api.server.services.DestinationService
import io.airbyte.api.server.services.UserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import java.util.UUID
import javax.ws.rs.core.Response

@Controller(DESTINATIONS_PATH)
open class DestinationsController(
  private val destinationService: DestinationService,
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
) : DestinationsApi {
  override fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

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
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun deleteDestination(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.deleteDestination(
            destinationId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun getDestination(
    destinationId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.getDestination(
            destinationId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun listDestinations(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val destinations: Any? =
      trackingHelper.callWithTracker({
        destinationService.listDestinationsForWorkspaces(
          safeWorkspaceIds,
          includeDeleted!!,
          limit!!,
          offset!!,
          authorization,
          getLocalUserInfoIfNull(userInfo),
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

  @Patch
  override fun patchDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeDestinationType(destinationPatchRequest)

    val destinationResponse: Any =
      trackingHelper.callWithTracker(
        {
          destinationService.partialUpdateDestination(
            destinationId,
            destinationPatchRequest,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun putDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    removeDestinationType(destinationPutRequest)

    val destinationResponse: Any? =
      trackingHelper.callWithTracker(
        {
          destinationService.updateDestination(
            destinationId,
            destinationPutRequest,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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
