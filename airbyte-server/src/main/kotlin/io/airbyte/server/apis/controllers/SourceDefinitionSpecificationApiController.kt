/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SourceDefinitionSpecificationApi
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.SourceDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
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
open class SourceDefinitionSpecificationApiController(
  private val connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
) : SourceDefinitionSpecificationApi {
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSourceDefinitionSpecification(
    @Body sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId,
  ): SourceDefinitionSpecificationRead =
    execute {
      connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(
        sourceDefinitionIdWithWorkspaceId,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSpecificationForSourceId(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): SourceDefinitionSpecificationRead =
    execute {
      connectorDefinitionSpecificationHandler.getSpecificationForSourceId(
        sourceIdRequestBody,
      )
    }
}
