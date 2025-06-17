/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OpenapiApi
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.OpenApiConfigHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.io.File

@Controller("/api/v1/openapi")
@Secured(SecurityRule.IS_AUTHENTICATED)
class OpenapiApiController(
  private val openApiConfigHandler: OpenApiConfigHandler,
) : OpenapiApi {
  @Get(produces = ["text/plain"])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOpenApiSpec(): File? = execute { openApiConfigHandler.file }
}
