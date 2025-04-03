/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ListPartialUserConfigsRequest
import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.PartialUserConfigListItem
import io.airbyte.api.model.generated.PartialUserConfigRead
import io.airbyte.api.model.generated.PartialUserConfigReadList
import io.airbyte.api.model.generated.PartialUserConfigRequestBody
import io.airbyte.api.model.generated.PartialUserConfigUpdate
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.data.services.PartialUserConfigService
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
  private val partialUserConfigService: PartialUserConfigService,
) {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listPartialUserConfigs(
    @Body listPartialUserConfigRequestBody: ListPartialUserConfigsRequest,
  ): PartialUserConfigReadList = partialUserConfigService.listPartialUserConfigs(listPartialUserConfigRequestBody.workspaceId).toApiModel()

  @Post("/create")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun createPartialUserConfig(
    @Body partialUserConfigCreate: PartialUserConfigCreate,
  ): PartialUserConfigRead =
    partialUserConfigWithActorDetailsToApiModel(
      partialUserConfigHandler.createPartialUserConfig(partialUserConfigCreate.toConfigModel()),
    )

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun updatePartialUserConfig(
    @Body partialUserConfigUpdate: PartialUserConfigUpdate,
  ): PartialUserConfigRead =
    partialUserConfigWithActorDetailsToApiModel(
      partialUserConfigHandler.updatePartialUserConfig(partialUserConfigUpdate.toConfigModel()),
    )

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getPartialUserConfig(
    @Body partialUserConfigRequestBody: PartialUserConfigRequestBody,
  ): PartialUserConfigRead =
    partialUserConfigWithActorDetailsToApiModel(partialUserConfigService.getPartialUserConfig(partialUserConfigRequestBody.partialUserConfigId))

  private fun PartialUserConfigCreate.toConfigModel(): PartialUserConfig =
    PartialUserConfig(
      id = UUID.randomUUID(),
      workspaceId = this.workspaceId,
      configTemplateId = this.configTemplateId,
      partialUserConfigProperties = this.partialUserConfigProperties,
    )

  private fun PartialUserConfigUpdate.toConfigModel(): PartialUserConfig {
    val existingPartialUserConfig = partialUserConfigService.getPartialUserConfig(this.partialUserConfigId)

    return PartialUserConfig(
      id = UUID.randomUUID(),
      workspaceId = existingPartialUserConfig.partialUserConfig.workspaceId,
      configTemplateId = existingPartialUserConfig.partialUserConfig.configTemplateId,
      partialUserConfigProperties = this.partialUserConfigProperties,
    )
  }

  private fun List<PartialUserConfigWithActorDetails>.toApiModel(): PartialUserConfigReadList {
    val items =
      this.map { partialUserConfig ->
        PartialUserConfigListItem()
          .partialUserConfigId(partialUserConfig.partialUserConfig.id)
          .configTemplateId(partialUserConfig.partialUserConfig.configTemplateId)
          .configTemplateIcon(partialUserConfig.actorIcon)
          .configTemplateName(partialUserConfig.actorName)
      }
    return PartialUserConfigReadList().partialUserConfigs(items)
  }

  private fun partialUserConfigWithActorDetailsToApiModel(partialUserConfig: PartialUserConfigWithActorDetails): PartialUserConfigRead =
    PartialUserConfigRead()
      .partialUserConfigId(
        partialUserConfig.partialUserConfig.id,
      ).configTemplateId(partialUserConfig.partialUserConfig.configTemplateId)
      .sourceId(partialUserConfig.partialUserConfig.sourceId)
      .partialUserConfigProperties(partialUserConfig.partialUserConfig.partialUserConfigProperties)
}
