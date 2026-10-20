/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DestinationDefinitionSpecificationApi
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationDefinitionSpecificationApiController(
  private val connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
) : DestinationDefinitionSpecificationApi {
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDestinationDefinitionSpecification(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionSpecificationRead =
    execute {
      connectorDefinitionSpecificationHandler.getDestinationSpecification(
        destinationDefinitionIdWithWorkspaceId,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getSpecificationForDestinationId(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationDefinitionSpecificationRead =
    execute {
      connectorDefinitionSpecificationHandler.getSpecificationForDestinationId(
        destinationIdRequestBody,
      )
    }
}
