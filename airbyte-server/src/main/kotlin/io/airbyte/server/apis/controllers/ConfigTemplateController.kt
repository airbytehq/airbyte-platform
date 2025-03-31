/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConfigTemplateList
import io.airbyte.api.model.generated.ConfigTemplateListItem
import io.airbyte.api.model.generated.ConfigTemplateListRequest
import io.airbyte.api.model.generated.ConfigTemplateRead
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.server.handlers.ConfigTemplateHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/config_templates")
class ConfigTemplateController(
  private val configTemplateHandler: ConfigTemplateHandler,
) {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listConfigTemplates(
    @Body configTemplateListRequest: ConfigTemplateListRequest,
  ): ConfigTemplateList =
    configTemplateListToApiModel(configTemplateHandler.listConfigTemplatesForOrganization(configTemplateListRequest.organizationId))

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getConfigTemplate(
    @Body configTemplateRequestBody: ConfigTemplateRequestBody,
  ): ConfigTemplateRead = configTemplateToApiModel(configTemplateHandler.getConfigTemplate(configTemplateRequestBody.configTemplateId))

  private fun configTemplateListToApiModel(configTemplateListItems: List<ConfigTemplateWithActorDetails>): ConfigTemplateList {
    val configTemplateList = ConfigTemplateList()
    configTemplateList.configTemplates =
      configTemplateListItems
        .map { item ->
          ConfigTemplateListItem()
            .id(item.configTemplate.id)
            .name(item.actorName)
            .icon(item.actorIcon)
        }
    return configTemplateList
  }

  private fun configTemplateToApiModel(configTemplate: ConfigTemplateWithActorDetails): ConfigTemplateRead =
    ConfigTemplateRead()
      .id(configTemplate.configTemplate.id)
      .name(configTemplate.actorName)
      .icon(configTemplate.actorIcon)
      .sourceDefinitionId(configTemplate.configTemplate.actorDefinitionId)
      .configTemplateSpec(configTemplate.configTemplate.userConfigSpec)
}
