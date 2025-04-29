/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonMergingHelper
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val sourceHandler: SourceHandler,
  @Named("jsonSecretsProcessorWithCopy") val secretsProcessor: JsonSecretsProcessor,
) {
  private val jsonMergingHelper = JsonMergingHelper()

  /**
   * Creates a partial user config and its associated source.
   *
   * @param partialUserConfigCreate The updated partial user config
   * @return The created partial user config with actor details
   */
  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithConfigTemplateAndActorDetails {
    // Get the config template and actor definition
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigCreate.configTemplateId)

    // Create and persist the source
    val combinedConfigs =
      jsonMergingHelper.combineProperties(
        configTemplate.configTemplate.partialDefaultConfig,
        partialUserConfigCreate.connectionConfiguration,
      )
    val sourceCreate =
      createSourceCreateFromPartialUserConfig(configTemplate.configTemplate, partialUserConfigCreate, combinedConfigs, configTemplate.actorName)
    val savedSource = sourceHandler.createSource(sourceCreate)

    // Handle secrets in configuration
    val connectorSpec =
      ConnectorSpecification().apply {
        connectionSpecification = configTemplate.configTemplate.userConfigSpec.connectionSpecification
      }
    val connectionConfig = partialUserConfigCreate.connectionConfiguration
    val secureConfig =
      sourceHandler.persistConfigRawSecretValues(
        connectionConfig,
        Optional.empty(),
        partialUserConfigCreate.workspaceId,
        connectorSpec,
        savedSource.sourceId,
      )

    // Save the secure config
    val securePartialUserConfig = partialUserConfigCreate.copy(connectionConfiguration = secureConfig, actorId = savedSource.sourceId)
    val createdPartialUserConfig = partialUserConfigService.createPartialUserConfig(securePartialUserConfig)

    return PartialUserConfigWithConfigTemplateAndActorDetails(
      partialUserConfig = createdPartialUserConfig.partialUserConfig,
      configTemplate = configTemplate.configTemplate,
      actorName = createdPartialUserConfig.actorName,
      actorIcon = createdPartialUserConfig.actorIcon,
    )
  }

  /**
   * Gets an existing partial user config.
   *
   * @param partialUserConfigId The id of the partial user config
   * @return The fetched partial user config with its template
   */
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithConfigTemplateAndActorDetails {
    val partialUserConfig = partialUserConfigService.getPartialUserConfig(partialUserConfigId)

    val sanitizedConfigProperties =
      secretsProcessor.prepareSecretsForOutput(
        partialUserConfig.partialUserConfig.connectionConfiguration,
        partialUserConfig.configTemplate.userConfigSpec.connectionSpecification,
      )

    partialUserConfig.partialUserConfig.connectionConfiguration = sanitizedConfigProperties

    return partialUserConfig
  }

  /**
   * Updates an existing partial user config and its associated source.
   *
   * @param partialUserConfig The updated partial user config
   * @return The updated partial user config with actor details
   */
  fun updatePartialUserConfig(partialUserConfig: PartialUserConfig): PartialUserConfigWithConfigTemplateAndActorDetails {
    // First get the existing config to verify it exists
    val existingConfig =
      partialUserConfigService
        .getPartialUserConfig(partialUserConfig.id)
        .partialUserConfig

    // Get the config template to use for merging properties
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfig.configTemplateId)

    // Combine the template's default config and user config
    val combinedConfigs =
      jsonMergingHelper.combineProperties(
        ObjectMapper().valueToTree(configTemplate.configTemplate.partialDefaultConfig),
        ObjectMapper().valueToTree(partialUserConfig.connectionConfiguration),
      )

    // Update the source using the combined config
    // "Partial update" allows us to update the configuration without changing the name or other properties on the source

    sourceHandler.partialUpdateSource(
      PartialSourceUpdate().apply {
        sourceId = existingConfig.actorId
        connectionConfiguration = combinedConfigs
      },
    )

    // Handle secrets in configuration
    val connectorSpec =
      ConnectorSpecification().apply {
        connectionSpecification = configTemplate.configTemplate.userConfigSpec.connectionSpecification
      }

    val connectionConfig = partialUserConfig.connectionConfiguration
    val secureConfig =
      sourceHandler.persistConfigRawSecretValues(
        connectionConfig,
        Optional.empty(),
        partialUserConfig.workspaceId,
        connectorSpec,
        existingConfig.actorId,
      )

    val securePartialUserConfig = partialUserConfig.copy(connectionConfiguration = secureConfig, actorId = existingConfig.actorId)

    // Update the partial user config in the database
    val updatedPartialUserConfig = partialUserConfigService.updatePartialUserConfig(securePartialUserConfig)

    return PartialUserConfigWithConfigTemplateAndActorDetails(
      partialUserConfig = updatedPartialUserConfig.partialUserConfig,
      configTemplate = configTemplate.configTemplate,
      actorName = updatedPartialUserConfig.actorName,
      actorIcon = updatedPartialUserConfig.actorIcon,
    )
  }

  private fun createSourceCreateFromPartialUserConfig(
    configTemplate: ConfigTemplate,
    partialUserConfig: PartialUserConfig,
    combinedConfigs: JsonNode,
    actorName: String,
  ): SourceCreate =
    SourceCreate().apply {
      name = "$actorName ${partialUserConfig.workspaceId}"
      sourceDefinitionId = configTemplate.actorDefinitionId
      workspaceId = partialUserConfig.workspaceId
      connectionConfiguration = combinedConfigs
    }
}
