/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DestinationDefinitionSpecificationApi
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/destination_definition_specifications")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationDefinitionSpecificationApiController(
  private val connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
) : DestinationDefinitionSpecificationApi {
  @Post("/get")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinitionSpecification(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionSpecificationRead? =
    execute {
      connectorDefinitionSpecificationHandler.getDestinationSpecification(
        destinationDefinitionIdWithWorkspaceId,
      )
    }

  @Post("/get_for_destination")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSpecificationForDestinationId(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationDefinitionSpecificationRead? =
    execute {
      connectorDefinitionSpecificationHandler.getSpecificationForDestinationId(
        destinationIdRequestBody,
      )
    }
}
