/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.PartialUserConfigRead
import io.airbyte.api.model.generated.PartialUserConfigRequestBody
import io.airbyte.api.model.generated.PartialUserConfigUpdate
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithSourceId
import io.airbyte.server.handlers.PartialUserConfigHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.util.UUID

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
  ): PartialUserConfigRead =
    partialUserConfigWithSourceIdToApiModel(partialUserConfigHandler.createPartialUserConfig(createPartialUserConfigEntity(partialUserConfigCreate)))

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

  private fun createPartialUserConfigEntity(partialUserConfigCreate: PartialUserConfigCreate): PartialUserConfig =
    PartialUserConfig(
      id = UUID.randomUUID(),
      workspaceId = partialUserConfigCreate.workspaceId,
      configTemplateId = partialUserConfigCreate.configTemplateId,
      partialUserConfigProperties = partialUserConfigCreate.partialUserConfigProperties,
    )

  private fun partialUserConfigWithSourceIdToApiModel(partialUserConfigWithSourceId: PartialUserConfigWithSourceId): PartialUserConfigRead =
    PartialUserConfigRead()
      .partialUserConfigId(
        partialUserConfigWithSourceId.id,
      ).configTemplateId(partialUserConfigWithSourceId.configTemplateId)
      .sourceId(partialUserConfigWithSourceId.sourceId)
      .partialUserConfigProperties(partialUserConfigWithSourceId.partialUserConfigProperties)
}
