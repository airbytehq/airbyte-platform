/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithSourceId
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.protocol.models.v0.ConnectorSpecification
import jakarta.inject.Singleton

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val sourceHandler: SourceHandler,
) {
  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithSourceId {
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
      sourceHandler.persistSecretsAndUpdateSourceConnection(
        null,
        connectionConfig,
        partialUserConfigCreate.workspaceId,
        connectorSpec,
      )

    // Save the secure config
    val securePartialUserConfig = partialUserConfigCreate.copy(partialUserConfigProperties = secureConfig)
    val savedConfig = partialUserConfigService.createPartialUserConfig(securePartialUserConfig.toEntity()).toConfigModel()

    // Return with source ID
    return PartialUserConfigWithSourceId(
      id = savedConfig.id,
      workspaceId = savedConfig.workspaceId,
      configTemplateId = savedConfig.configTemplateId,
      partialUserConfigProperties = savedConfig.partialUserConfigProperties,
      sourceId = savedSource.sourceId,
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
