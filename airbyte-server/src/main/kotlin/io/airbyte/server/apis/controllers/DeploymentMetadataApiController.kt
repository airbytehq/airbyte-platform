/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DeploymentMetadataApi
import io.airbyte.api.server.generated.models.DeploymentMetadataRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED, AuthRoleConstants.DATAPLANE)
class DeploymentMetadataApiController(
  private val deploymentMetadataHandler: DeploymentMetadataHandler,
) : DeploymentMetadataApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDeploymentMetadata(): DeploymentMetadataRead = execute { deploymentMetadataHandler.getDeploymentMetadata() }
}
