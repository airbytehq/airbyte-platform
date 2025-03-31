/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ConfigTemplateHandler(
  private val configTemplateService: ConfigTemplateService,
  private val sourceService: SourceService,
) {
  fun listConfigTemplatesForOrganization(organizationId: UUID): List<ConfigTemplateWithActorDetails> {
    val configTemplates = configTemplateService.listConfigTemplates(organizationId).map { it.toConfigModel() }

    val configTemplateListItems: List<ConfigTemplateWithActorDetails> =
      configTemplates.map { it ->
        val actorDefinition = sourceService.getStandardSourceDefinition(it.actorDefinitionId, false)

        ConfigTemplateWithActorDetails(
          configTemplate = it,
          actorName = actorDefinition.name,
          actorIcon = actorDefinition.iconUrl,
        )
      }

    return configTemplateListItems
  }

  fun getConfigTemplate(configTemplateId: UUID): ConfigTemplateWithActorDetails {
    val configTemplateConfigModel = configTemplateService.getConfigTemplate(configTemplateId).toConfigModel()
    val actorDefinition = sourceService.getStandardSourceDefinition(configTemplateConfigModel.actorDefinitionId, false)

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplateConfigModel,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }
}
