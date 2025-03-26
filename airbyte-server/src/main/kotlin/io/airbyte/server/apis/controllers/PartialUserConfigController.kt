/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.PartialUserConfigRequestBody
import io.airbyte.api.model.generated.PartialUserConfigUpdate
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.handlers.PartialUserConfigHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/partial_user_configs")
class PartialUserConfigController(
  private val partialUserConfigHandler: PartialUserConfigHandler,
) {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listPartialUserConfigs(
    @Body listPartialUserConfigRequestBody: PartialUserConfigRequestBody,
  ) {
    // No-op
  }

  @Post("/create")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun createPartialUserConfig(
    @Body partialUserConfigCreate: PartialUserConfigCreate,
  ) = partialUserConfigHandler.createPartialUserConfig(partialUserConfigCreate)

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun updatePartialUserConfig(
    @Body partialUserConfigUpdate: PartialUserConfigUpdate,
  ) {
    // No-op
  }

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getPartialUserConfig(
    @Body partialUserConfigRequestBody: PartialUserConfigRequestBody,
  ) {
    // No-op
    // ALSO: fix annotations later
  }
}
