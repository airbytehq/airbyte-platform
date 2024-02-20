/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package controllers

import authorization.AirbyteApiAuthorizationHelper
import authorization.Scope
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.airbyte_api.generated.DestinationsApi
import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.api.model.generated.PermissionType
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
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID
import javax.ws.rs.Path
import javax.ws.rs.core.Response

@Controller(DESTINATIONS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationsController(
  private val destinationService: DestinationService,
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
  private val airbyteApiAuthorizationHelper: AirbyteApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : DestinationsApi {
  override fun createDestination(
    destinationCreateRequest: DestinationCreateRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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

  @Path("/{destinationId}")
  @Patch
  override fun patchDestination(
    destinationId: UUID,
    destinationPatchRequest: DestinationPatchRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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

  @Path("/{destinationId}")
  override fun putDestination(
    destinationId: UUID,
    destinationPutRequest: DestinationPutRequest,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
