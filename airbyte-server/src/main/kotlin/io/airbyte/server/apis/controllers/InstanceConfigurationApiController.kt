/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.InstanceConfigurationApi
import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody
import io.airbyte.api.model.generated.LicenseInfoResponse
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

// this controller is only usable in self-managed versions of Airbyte. Not Cloud!
@Controller("/api/v1/instance_configuration")
@Secured(SecurityRule.IS_ANONYMOUS)
class InstanceConfigurationApiController(
  private val instanceConfigurationHandler: InstanceConfigurationHandler,
) : InstanceConfigurationApi {
  @Get
  override fun getInstanceConfiguration(): InstanceConfigurationResponse? = execute { instanceConfigurationHandler.instanceConfiguration }

  override fun licenseInfo(): LicenseInfoResponse? = execute { instanceConfigurationHandler.licenseInfo() }

  override fun setupInstanceConfiguration(
    @Body instanceConfigurationSetupRequestBody: InstanceConfigurationSetupRequestBody,
  ): InstanceConfigurationResponse? =
    execute {
      instanceConfigurationHandler.setupInstanceConfiguration(
        instanceConfigurationSetupRequestBody,
      )
    }
}
