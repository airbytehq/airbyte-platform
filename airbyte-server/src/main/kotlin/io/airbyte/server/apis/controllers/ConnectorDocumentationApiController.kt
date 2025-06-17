/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ConnectorDocumentationApi
import io.airbyte.api.model.generated.ConnectorDocumentationRead
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectorDocumentationHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/connector_documentation")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorDocumentationApiController(
  @param:Body private val connectorDocumentationHandler: ConnectorDocumentationHandler,
) : ConnectorDocumentationApi {
  @Post
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorDocumentation(
    @Body connectorDocumentationRequestBody: ConnectorDocumentationRequestBody,
  ): ConnectorDocumentationRead? =
    execute {
      connectorDocumentationHandler.getConnectorDocumentation(
        connectorDocumentationRequestBody,
      )
    }
}
