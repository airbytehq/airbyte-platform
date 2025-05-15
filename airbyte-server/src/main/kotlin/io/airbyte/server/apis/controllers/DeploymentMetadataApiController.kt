/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DeploymentMetadataApi
import io.airbyte.api.model.generated.DeploymentMetadataRead
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

@Controller("/api/v1/deployment/metadata")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED, AuthRoleConstants.DATAPLANE)
class DeploymentMetadataApiController(
  @param:Body private val deploymentMetadataHandler: DeploymentMetadataHandler,
) : DeploymentMetadataApi {
  override fun getDeploymentMetadata(): DeploymentMetadataRead? = execute(Callable { deploymentMetadataHandler.deploymentMetadata })
}
