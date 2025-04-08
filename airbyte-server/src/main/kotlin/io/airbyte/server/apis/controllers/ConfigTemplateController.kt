/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ConfigTemplateApi
import io.airbyte.api.model.generated.ConfigTemplateList
import io.airbyte.api.model.generated.ConfigTemplateListItem
import io.airbyte.api.model.generated.ConfigTemplateRead
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.api.model.generated.ListConfigTemplatesRequestBody
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.OrganizationId
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/config_templates")
open class ConfigTemplateController(
  private val configTemplateService: ConfigTemplateService,
) : ConfigTemplateApi {
  @Post("/get")
  @Secured(SecurityRule.IS_AUTHENTICATED)
  override fun getConfigTemplate(configTemplateRequestBody: ConfigTemplateRequestBody): ConfigTemplateRead =
    configTemplateService.getConfigTemplate(configTemplateRequestBody.configTemplateId).toApiModel()

  @Post("/list")
  @Secured(SecurityRule.IS_AUTHENTICATED)
  override fun listConfigTemplates(listConfigTemplatesRequestBody: ListConfigTemplatesRequestBody): ConfigTemplateList {
    val configTemplateList = ConfigTemplateList()
    configTemplateList.configTemplates(
      configTemplateService
        .listConfigTemplatesForOrganization(OrganizationId(listConfigTemplatesRequestBody.organizationId))
        .map { it.toListItem() },
    )
    return configTemplateList
  }

  private fun ConfigTemplateWithActorDetails.toApiModel(): ConfigTemplateRead {
    val configTemplateRead = ConfigTemplateRead()
    configTemplateRead.sourceDefinitionId(this.configTemplate.actorDefinitionId)
    configTemplateRead.configTemplateSpec(this.configTemplate.userConfigSpec)
    configTemplateRead.icon(this.actorIcon)
    configTemplateRead.name(this.actorName)
    configTemplateRead.id(this.configTemplate.id)
    return configTemplateRead
  }

  private fun ConfigTemplateWithActorDetails.toListItem(): ConfigTemplateListItem {
    val listItem = ConfigTemplateListItem()
    listItem.id(this.configTemplate.id)
    listItem.icon(this.actorIcon)
    listItem.name(this.actorName)
    return listItem
  }
}
