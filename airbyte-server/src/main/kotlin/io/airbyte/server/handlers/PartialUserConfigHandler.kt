/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.cronutils.utils.VisibleForTesting
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithFullDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.objectMapper
import io.airbyte.validation.json.JsonMergingHelper
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val sourceHandler: SourceHandler,
) {
  private val jsonMergingHelper = JsonMergingHelper()

  /**
   * Creates a partial user config and its associated source.
   *
   * @param partialUserConfigCreate The updated partial user config
   * @return The created partial user config with actor details
   */
  fun createSourceFromPartialConfig(
    partialUserConfigCreate: PartialUserConfig,
    connectionConfiguration: JsonNode,
  ): SourceRead {
    // Get the config template and actor definition
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigCreate.configTemplateId)

    // Create and persist the source
    val combinedConfigs =
      jsonMergingHelper.combineProperties(
        configTemplate.configTemplate.partialDefaultConfig,
        connectionConfiguration,
      )
    val sourceCreate =
      createSourceCreateFromPartialUserConfig(
        configTemplate.configTemplate,
        partialUserConfigCreate,
        combinedConfigs,
        configTemplate.actorName,
      )
    val sourceRead = sourceHandler.createSource(sourceCreate)

    val partialUserConfigToPersist =
      PartialUserConfig(
        id = sourceRead.sourceId,
        workspaceId = partialUserConfigCreate.workspaceId,
        configTemplateId = configTemplate.configTemplate.id,
        actorId = sourceRead.sourceId,
      )

    partialUserConfigService.createPartialUserConfig(partialUserConfigToPersist)

    return sourceRead
  }

  /**
   * Gets a partial config from a source
   *
   * @param partialUserConfigId The id of the partial user config
   * @return The fetched partial user config with its template
   */
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithFullDetails {
    val partialUserConfigStored = partialUserConfigService.getPartialUserConfig(partialUserConfigId)

    val sourceRead = sourceHandler.getSource(SourceIdRequestBody().apply { this.sourceId = partialUserConfigStored.partialUserConfig.actorId })
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigStored.partialUserConfig.configTemplateId)

    val filteredConnectionConfiguration = filterConnectionConfigurationBySpec(sourceRead, configTemplate)

    return PartialUserConfigWithFullDetails(
      partialUserConfig =
        PartialUserConfig(
          id = partialUserConfigStored.partialUserConfig.id,
          workspaceId = partialUserConfigStored.partialUserConfig.workspaceId,
          configTemplateId = configTemplate.configTemplate.id,
          actorId = sourceRead.sourceId,
        ),
      connectionConfiguration = filteredConnectionConfiguration,
      configTemplate = configTemplate.configTemplate,
      actorName = sourceRead.name,
      actorIcon = sourceRead.icon,
    )
  }

  /**
   * Updates a source based on a partial user config.
   *
   * @param partialUserConfig The updated partial user config
   * @return The updated partial user config with actor details
   */
  fun updateSourceFromPartialConfig(
    partialUserConfig: PartialUserConfig,
    connectionConfiguration: JsonNode,
  ): SourceRead {
    val storedPartialUserConfig = partialUserConfigService.getPartialUserConfig(partialUserConfig.id)
    // Get the config template to use for merging properties
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfig.configTemplateId)

    // Combine the template's default config and user config
    val combinedConfigs =
      jsonMergingHelper.combineProperties(
        ObjectMapper().valueToTree(configTemplate.configTemplate.partialDefaultConfig),
        ObjectMapper().valueToTree(connectionConfiguration),
      )

    // Update the source using the combined config
    // "Partial update" allows us to update the configuration without changing the name or other properties on the source

    val sourceRead =
      sourceHandler.partialUpdateSource(
        PartialSourceUpdate()
          .sourceId(storedPartialUserConfig.partialUserConfig.actorId)
          .connectionConfiguration(combinedConfigs),
      )

    return sourceRead
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
   * Filters a source's connection configuration to only include
   * properties that are defined in the config template spec.
   *
   * @param sourceRead The SourceRead object containing the connection configuration
   * @param configTemplateRead The ConfigTemplateRead containing the user config specification
   * @return A JsonNode with only the properties specified in the template spec
   */
  @VisibleForTesting
  internal fun filterConnectionConfigurationBySpec(
    sourceRead: SourceRead,
    configTemplateRead: ConfigTemplateWithActorDetails,
  ): JsonNode {
    val connectionConfig = sourceRead.connectionConfiguration

    val userConfigSpec = configTemplateRead.configTemplate.userConfigSpec.connectionSpecification

    // Convert ConnectorSpecification to JsonNode
    val specAsJson = objectMapper.valueToTree<JsonNode>(userConfigSpec)

    // Filter the connection configuration based on the schema
    return filterJsonNodeBySchema(connectionConfig, specAsJson)
  }

  /**
   * Recursively filters a JsonNode according to a JSON schema
   */
  @VisibleForTesting
  internal fun filterJsonNodeBySchema(
    node: JsonNode,
    schema: JsonNode,
  ): JsonNode {
    // If schema doesn't exist or is not an object, return an empty object
    if (!schema.isObject) {
      return objectMapper.createObjectNode()
    }

    // Check schema type
    val schemaType = schema.get("type")?.asText()

    // Handle different schema types
    when (schemaType) {
      "object" -> {
        // Handle object schema
        val filteredNode = objectMapper.createObjectNode()

        // Process schema properties if they exist
        if (schema.has("properties") && node.isObject) {
          val propertiesSchema = schema.get("properties")

          for (fieldName in node.fieldNames().asSequence()) {
            // Check if this field is defined in the schema
            if (propertiesSchema.has(fieldName)) {
              val fieldValue = node.get(fieldName)
              val fieldSchema = propertiesSchema.get(fieldName)

              // Recursively filter the field
              filteredNode.set<JsonNode>(fieldName, filterJsonNodeBySchema(fieldValue, fieldSchema))
            }
          }
        }

        if (schema.has("oneOf")) {
          val oneOfArray = schema.get("oneOf")
          if (oneOfArray.isArray && node.isObject) {
            // Find which schema best matches our input
            var bestMatchSchema: JsonNode? = null
            var maxMatchCount = -1

            for (subSchema in oneOfArray) {
              if (!subSchema.has("properties")) continue

              val schemaProps = subSchema.get("properties")
              val nodeFields = node.fieldNames().asSequence().toSet()
              val schemaFields = schemaProps.fieldNames().asSequence().toSet()

              val commonFields = nodeFields.intersect(schemaFields)
              val matchScore = commonFields.size

              if (commonFields == nodeFields) {
                // Perfect match - all fields are covered
                bestMatchSchema = subSchema
                break
              } else if (matchScore > maxMatchCount) {
                bestMatchSchema = subSchema
                maxMatchCount = matchScore
              }
            }

            // If we found a matching schema, ONLY include properties from that schema
            if (bestMatchSchema != null && bestMatchSchema.has("properties")) {
              val resultNode = objectMapper.createObjectNode()
              val schemaProps = bestMatchSchema.get("properties")
              val schemaFieldNames = schemaProps.fieldNames()

              // Only include properties explicitly defined in the schema
              while (schemaFieldNames.hasNext()) {
                val propName = schemaFieldNames.next()
                if (node.has(propName)) {
                  val propSchema = schemaProps.get(propName)
                  resultNode.set<JsonNode>(propName, filterJsonNodeBySchema(node.get(propName), propSchema))
                }
              }

              return resultNode // Return only the properties defined in the matching schema
            }
          }
        }
        return filteredNode
      }
      "array" -> {
        // For arrays, if node is an array, process each element with the items schema
        if (node.isArray && schema.has("items")) {
          val itemsSchema = schema.get("items")
          val arrayNode = objectMapper.createArrayNode()

          node.forEach { element ->
            arrayNode.add(filterJsonNodeBySchema(element, itemsSchema))
          }

          return arrayNode
        }

        // If the node is not an array but schema expects array, return empty array
        return objectMapper.createArrayNode()
      }
      else -> {
        // For primitive types (string, number, boolean, null) or when type is not specified,
        // simply return the node
        return node
      }
    }
  }
}
