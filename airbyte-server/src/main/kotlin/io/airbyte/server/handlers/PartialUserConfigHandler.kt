/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
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
    val combinedConfigs = combineProperties(configTemplate.configTemplate.partialDefaultConfig, partialUserConfigCreate.partialUserConfigProperties)
    val sourceCreate =
      createSourceCreateFromPartialUserConfig(configTemplate.configTemplate, partialUserConfigCreate, combinedConfigs, configTemplate.actorName)
    val savedSource = sourceHandler.createSource(sourceCreate)

    // Handle secrets in configuration
    val connectorSpec =
      ConnectorSpecification().apply {
        connectionSpecification = configTemplate.configTemplate.userConfigSpec.get("connectionSpecification")
      }
    val connectionConfig = partialUserConfigCreate.partialUserConfigProperties.get("connectionConfiguration")
    val secureConfig =
      sourceHandler.persistConfigRawSecretValues(
        connectionConfig,
        Optional.empty(),
        partialUserConfigCreate.workspaceId,
        connectorSpec,
        savedSource.sourceId,
      )

    // Save the secure config
    val securePartialUserConfig = partialUserConfigCreate.copy(partialUserConfigProperties = secureConfig, actorId = savedSource.sourceId)
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
        partialUserConfig.partialUserConfig.partialUserConfigProperties,
        partialUserConfig.configTemplate.userConfigSpec["connectionSpecification"],
      )

    partialUserConfig.partialUserConfig.partialUserConfigProperties = sanitizedConfigProperties

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
      combineProperties(
        ObjectMapper().valueToTree(configTemplate.configTemplate.partialDefaultConfig),
        ObjectMapper().valueToTree(partialUserConfig.partialUserConfigProperties),
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
        connectionSpecification = configTemplate.configTemplate.userConfigSpec.get("connectionSpecification")
      }

    val connectionConfig = partialUserConfig.partialUserConfigProperties.get("connectionConfiguration")
    val secureConfig =
      sourceHandler.persistConfigRawSecretValues(
        connectionConfig,
        Optional.empty(),
        partialUserConfig.workspaceId,
        connectorSpec,
        existingConfig.actorId,
      )

    val securePartialUserConfig = partialUserConfig.copy(partialUserConfigProperties = secureConfig, actorId = existingConfig.actorId)

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

  /**
   * Combines all properties from ConfigTemplate.partialDefaultConfig and
   * PartialUserConfig.partialUserConfigProperties into a single JSON object.
   * Recursively merges the JSON structures, with user config values taking precedence.
   */
  internal fun combineProperties(
    configTemplateNode: JsonNode,
    partialUserConfigNode: JsonNode,
  ): JsonNode {
    val objectMapper = ObjectMapper()
    val combinedNode = objectMapper.createObjectNode()

    // Helper function to merge two JSON objects recursively
    fun mergeObjects(
      target: ObjectNode,
      source: ObjectNode,
    ) {
      source.fields().forEach { (key, value) ->
        if (target.has(key)) {
          val targetValue = target.get(key)
          if (targetValue.isObject && value.isObject) {
            // Both are objects, merge them recursively
            val targetObjectNode = targetValue as ObjectNode
            val sourceObjectNode = value as ObjectNode
            mergeObjects(targetObjectNode, sourceObjectNode)
          } else if (targetValue.isObject != value.isObject) {
            // One is an object, the other isn't - this is a type conflict
            throw IllegalArgumentException(
              "Type mismatch for property '$key': Cannot merge object with non-object",
            )
          } else {
            // Neither is an object, just override
            target.set<JsonNode>(key, value)
          }
        } else {
          // Key doesn't exist in target, just set it
          target.set<JsonNode>(key, value)
        }
      }
    }

    // First, extract the connectionConfiguration objects from both nodes
    val defaultConfigNode =
      if (configTemplateNode.has("connectionConfiguration")) {
        configTemplateNode.get("connectionConfiguration") as? ObjectNode
      } else {
        null
      }

    val userConfigNode =
      if (partialUserConfigNode.has("connectionConfiguration")) {
        partialUserConfigNode.get("connectionConfiguration") as? ObjectNode
      } else {
        null
      }

    // Apply default configuration first
    defaultConfigNode?.let { mergeObjects(combinedNode, it) }

    // Then apply user configuration (which will override defaults where they overlap)
    userConfigNode?.let { mergeObjects(combinedNode, it) }

    return combinedNode
  }
}
