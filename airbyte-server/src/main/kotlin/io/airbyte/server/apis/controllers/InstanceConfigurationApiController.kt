/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.InstanceConfigurationApi
import io.airbyte.api.server.generated.models.InstanceConfigurationResponse
import io.airbyte.api.server.generated.models.InstanceConfigurationSetupRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

// this controller is only usable in self-managed versions of Airbyte. Not Cloud!
@Controller
@Secured(SecurityRule.IS_ANONYMOUS)
class InstanceConfigurationApiController(
  private val instanceConfigurationHandler: InstanceConfigurationHandler,
) : InstanceConfigurationApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PUBLIC,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun getInstanceConfiguration(): InstanceConfigurationResponse = execute { instanceConfigurationHandler.instanceConfiguration }

  @AuthorizeOperation(
    access = RouteAccessPolicy.LOGIN,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun setupInstanceConfiguration(
    @Body instanceConfigurationSetupRequestBody: InstanceConfigurationSetupRequestBody?,
  ): InstanceConfigurationResponse =
    execute {
      instanceConfigurationHandler.setupInstanceConfiguration(
        instanceConfigurationSetupRequestBody!!,
      )
    }
}
