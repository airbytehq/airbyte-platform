/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConfigTemplateListRequest
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/config_templates")
class ConfigTemplateController {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listConfigTemplates(
    @Body configTemplateListRequest: ConfigTemplateListRequest,
  ) {
    // No-op
  }

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getConfigTemplate(
    @Body configTemplateRequestBody: ConfigTemplateRequestBody,
  ) {
    // No-op
  }
}
