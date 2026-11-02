/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DeploymentMetadataApi
import io.airbyte.api.server.generated.models.DeploymentMetadataRead
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.DeploymentMetadataRule
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
class DeploymentMetadataApiController(
  private val deploymentMetadataHandler: DeploymentMetadataHandler,
) : DeploymentMetadataApi {
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = DeploymentMetadataRule::class,
  )
  override fun getDeploymentMetadata(): DeploymentMetadataRead = execute { deploymentMetadataHandler.getDeploymentMetadata() }
}
