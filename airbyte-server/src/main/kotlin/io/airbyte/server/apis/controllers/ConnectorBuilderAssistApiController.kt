/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ConnectorBuilderAssistApi
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.AssistProxyHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.AuthenticatedMetadataRule
import io.micronaut.context.annotation.Context
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.server.cors.CrossOrigin
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/connector_builder_assist")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorBuilderAssistApiController(
  private val assistProxyHandler: AssistProxyHandler,
) : ConnectorBuilderAssistApi {
  @Post(uri = "/process", consumes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun assistV1Process(
    @Body requestBody: Map<String, Any>,
  ): Map<String, Any> = assistProxyHandler.process(requestBody, true)

  @Post(uri = "/warm", consumes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  @Secured(SecurityRule.IS_ANONYMOUS)
  @CrossOrigin("*")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PUBLIC,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun assistV1Warm(
    @Body requestBody: Map<String, Any>,
  ): Map<String, Any> = assistProxyHandler.process(requestBody, false)
}
