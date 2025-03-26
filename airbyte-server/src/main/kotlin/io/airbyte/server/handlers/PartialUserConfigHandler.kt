/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.PartialUserConfigRead
import io.airbyte.api.model.generated.PartialUserConfigWithSource
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.data.repositories.entities.PartialUserConfig
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceHandler: SourceHandler,
) {
  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfigCreate): PartialUserConfigWithSource {
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
    val partialUserConfig = createPartialUserConfigEntity(partialUserConfigCreate)

    val savedSource = sourceHandler.createSource(sourceCreate)
    val savedPartialUserConfig = partialUserConfigService.createPartialUserConfig(partialUserConfig)

    return buildResponse(savedSource, savedPartialUserConfig)
  }

  private fun createSourceCreateFromPartialUserConfig(
    configTemplate: ConfigTemplate,
    partialUserConfigCreate: PartialUserConfigCreate,
    combinedConfigs: JsonNode,
    actorNameOrElseUUID: String?,
  ): SourceCreate =
    SourceCreate().apply {
      name = "$actorNameOrElseUUID ${partialUserConfigCreate.workspaceId}"
      sourceDefinitionId = configTemplate.actorDefinitionId
      workspaceId = partialUserConfigCreate.workspaceId
      connectionConfiguration = combinedConfigs
    }

  private fun createPartialUserConfigEntity(partialUserConfigCreate: PartialUserConfigCreate): PartialUserConfig =
    PartialUserConfig(
      id = UUID.randomUUID(),
      workspaceId = partialUserConfigCreate.workspaceId,
      configTemplateId = partialUserConfigCreate.configTemplateId,
      partialUserConfigProperties = partialUserConfigCreate.partialUserConfigProperties,
      tombstone = false,
    )

  private fun buildResponse(
    savedSource: SourceRead,
    savedPartialUserConfig: PartialUserConfig,
  ): PartialUserConfigWithSource =
    PartialUserConfigWithSource().apply {
      partialUserConfig =
        PartialUserConfigRead()
          .partialUserConfigId(savedPartialUserConfig.id)
          .configTemplateId(savedPartialUserConfig.configTemplateId)
          .partialUserConfigProperties(savedPartialUserConfig.partialUserConfigProperties)
          .sourceId(savedSource.sourceId)

      source =
        SourceRead()
          .sourceId(savedSource.sourceId)
          .connectionConfiguration(savedSource.connectionConfiguration)
          .name(savedSource.name)
          .sourceDefinitionId(savedSource.sourceDefinitionId)
    }

  /**
   * Combines all properties from ConfigTemplate.partialDefaultConfig and
   * PartialUserConfig.partialUserConfigProperties into a single JSON object.
   * Simply concatenates all properties without worrying about overlaps.
   */
  internal fun combineProperties(
    configTemplateNode: JsonNode,
    partialUserConfigNode: JsonNode,
  ): JsonNode {
    val objectMapper = ObjectMapper()
    val combinedNode = objectMapper.createObjectNode()

    // Helper function to merge two JSON objects
    fun mergeObjects(
      target: ObjectNode,
      source: ObjectNode,
    ) {
      source.fields().forEach { (key, value) ->
        if (target.has(key) && target.get(key).isObject && value.isObject) {
          mergeObjects(target.get(key) as ObjectNode, value as ObjectNode)
        } else {
          target.set<JsonNode>(key, value)
        }
      }
    }

    // If configTemplateNode has a connectionConfiguration field, use that
    if (configTemplateNode.has("connectionConfiguration")) {
      val defaultConfig = configTemplateNode.get("connectionConfiguration")
      if (defaultConfig is ObjectNode) {
        mergeObjects(combinedNode, defaultConfig)
      }
    }

    // If partialUserConfigNode has a connectionConfiguration field, use that
    if (partialUserConfigNode.has("connectionConfiguration")) {
      val userConfig = partialUserConfigNode.get("connectionConfiguration")
      if (userConfig is ObjectNode) {
        mergeObjects(combinedNode, userConfig)
      }
    }

    return combinedNode
  }
}
