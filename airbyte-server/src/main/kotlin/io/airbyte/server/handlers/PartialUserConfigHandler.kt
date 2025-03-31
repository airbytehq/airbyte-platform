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
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import jakarta.inject.Singleton

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceHandler: SourceHandler,
) {
  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithSourceId {
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigCreate.configTemplateId).toConfigModel()
    val actorDefinition =
      actorDefinitionService
        .getDefaultVersionForActorDefinitionIdOptional(configTemplate.actorDefinitionId)
        .orElse(null)

    val actorNameOrElseUUID =
      actorDefinition
        ?.additionalProperties
        ?.get("name") as? String
        ?: configTemplate.actorDefinitionId.toString()

    val combinedConfigs = combineProperties(configTemplate.partialDefaultConfig, partialUserConfigCreate.partialUserConfigProperties)

    val sourceCreate = createSourceCreateFromPartialUserConfig(configTemplate, partialUserConfigCreate, combinedConfigs, actorNameOrElseUUID)

    val savedSource = sourceHandler.createSource(sourceCreate)
    val savedPartialUserConfig = partialUserConfigService.createPartialUserConfig(partialUserConfigCreate.toEntity()).toConfigModel()

    val partialUserConfigWithSourceId =
      PartialUserConfigWithSourceId(
        id = savedPartialUserConfig.id,
        workspaceId = savedPartialUserConfig.workspaceId,
        configTemplateId = savedPartialUserConfig.configTemplateId,
        partialUserConfigProperties = savedPartialUserConfig.partialUserConfigProperties,
        sourceId = savedSource.sourceId,
      )

    return partialUserConfigWithSourceId
  }

  private fun createSourceCreateFromPartialUserConfig(
    configTemplate: ConfigTemplate,
    partialUserConfig: PartialUserConfig,
    combinedConfigs: JsonNode,
    actorNameOrElseUUID: String?,
  ): SourceCreate =
    SourceCreate().apply {
      name = "$actorNameOrElseUUID ${partialUserConfig.workspaceId}"
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
