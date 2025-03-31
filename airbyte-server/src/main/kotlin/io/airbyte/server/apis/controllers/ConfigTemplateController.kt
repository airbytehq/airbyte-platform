/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.client.model.generated.ConfigTemplateUpdateRequestBody
import io.airbyte.api.model.generated.ConfigTemplateCreateRequestBody
import io.airbyte.api.model.generated.ConfigTemplateCreateResponse
import io.airbyte.api.model.generated.ConfigTemplateList
import io.airbyte.api.model.generated.ConfigTemplateListItem
import io.airbyte.api.model.generated.ConfigTemplateListRequest
import io.airbyte.api.model.generated.ConfigTemplateRead
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/config_templates")
class ConfigTemplateController(
  private val configTemplateService: ConfigTemplateService,
) {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listConfigTemplates(
    @Body configTemplateListRequest: ConfigTemplateListRequest,
  ): ConfigTemplateList =
    configTemplateListToApiModel(configTemplateService.listConfigTemplatesForOrganization(OrganizationId(configTemplateListRequest.organizationId)))

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getConfigTemplate(
    @Body configTemplateRequestBody: ConfigTemplateRequestBody,
  ): ConfigTemplateRead = configTemplateToApiModel(configTemplateService.getConfigTemplate(configTemplateRequestBody.configTemplateId))

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

  @Post("/create")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun createConfigTemplate(
    @Body createConfigTemplateRequestBody: ConfigTemplateCreateRequestBody,
  ): ConfigTemplateCreateResponse {
    val configTemplate =
      configTemplateService.createTemplate(
        organizationId = OrganizationId(createConfigTemplateRequestBody.organizationId),
        actorDefinitionId = ActorDefinitionId(createConfigTemplateRequestBody.actorDefinitionId),
        partialDefaultConfig = createConfigTemplateRequestBody.partialDefaultConfig,
        userConfigSpec = createConfigTemplateRequestBody.partialUserConfigSpec,
      )

    return configTemplate.toApiModel()
  }

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun updateConfigTemplate(
    @Body updateConfigTemplateRequestBody: ConfigTemplateUpdateRequestBody,
  ): ConfigTemplateCreateResponse {
    val configTemplate =
      configTemplateService.updateTemplate(
        configTemplateId = updateConfigTemplateRequestBody.configTemplateId,
        name = updateConfigTemplateRequestBody.name,
        partialDefaultConfig = updateConfigTemplateRequestBody.partialDefaultConfig,
        userConfigSpec = updateConfigTemplateRequestBody.partialUserConfigSpec,
      )

    return configTemplate.toApiModel()
  }

  private fun ConfigTemplate.toApiModel(): ConfigTemplateCreateResponse {
    val configTemplateCreateResponse = ConfigTemplateCreateResponse()
    configTemplateCreateResponse.id = this.id
    return configTemplateCreateResponse
  }

  private fun configTemplateToApiModel(configTemplate: ConfigTemplateWithActorDetails): ConfigTemplateRead =
    ConfigTemplateRead()
      .id(configTemplate.configTemplate.id)
      .name(configTemplate.actorName)
      .icon(configTemplate.actorIcon)
      .sourceDefinitionId(configTemplate.configTemplate.actorDefinitionId)
      .configTemplateSpec(configTemplate.configTemplate.userConfigSpec)
}
