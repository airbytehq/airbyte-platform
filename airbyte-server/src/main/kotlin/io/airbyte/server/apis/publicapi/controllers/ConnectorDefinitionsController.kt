/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.ConnectorDefinitionsApi
import io.airbyte.publicApi.server.generated.models.ConnectorType
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.CONNECTOR_DEFINITIONS_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.services.ConnectorDefinitionsService
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorDefinitionsController(
  val apiAuthorizationHelper: ApiAuthorizationHelper,
  val connectorDefinitionsService: ConnectorDefinitionsService,
  val currentUserService: CurrentUserService,
  val trackingHelper: TrackingHelper,
) : ConnectorDefinitionsApi {
  override fun listConnectorDefinitions(
    type: ConnectorType,
    workspaceId: UUID?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    when (workspaceId) {
      null -> {
        throw IllegalArgumentException("Workspace ID must be provided.")
      }
      workspaceId -> {
        apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
          Scope.WORKSPACE,
          listOf(workspaceId.toString()),
          setOf(WorkspaceAuthRole.WORKSPACE_READER),
        )
      }
    }

    val response =
      trackingHelper.callWithTracker(
        {
          connectorDefinitionsService.listConnectorDefinitions(
            type,
            workspaceId,
          )
        },
        CONNECTOR_DEFINITIONS_PATH,
        GET,
        userId,
      )

    trackingHelper.trackSuccess(
      CONNECTOR_DEFINITIONS_PATH,
      GET,
      userId,
      workspaceId,
    )

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(response)
      .build()
  }
}
