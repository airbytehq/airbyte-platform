/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.repositories.ConfigTemplateRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.data.mappers.EntityConfigTemplate
import io.airbyte.data.services.impls.data.mappers.objectMapper
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class ConfigTemplateServiceDataImpl(
  private val repository: ConfigTemplateRepository,
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceService: SourceService,
  private val validator: JsonSchemaValidator,
) : ConfigTemplateService {
  override fun getConfigTemplate(configTemplateId: UUID): ConfigTemplateWithActorDetails {
    val configTemplate = repository.findById(configTemplateId).orElseThrow().toConfigModel()
    val actorDefinition = sourceService.getStandardSourceDefinition(configTemplate.actorDefinitionId, false)

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplate,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }

  override fun listConfigTemplatesForOrganization(organizationId: OrganizationId): List<ConfigTemplateWithActorDetails> {
    val configTemplates = repository.findByOrganizationId(organizationId.value).map { it.toConfigModel() }

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

  override fun createTemplate(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    partialDefaultConfig: JsonNode,
    userConfigSpec: JsonNode,
  ): ConfigTemplateWithActorDetails {
    val convertedSpec = Jsons.deserialize(userConfigSpec.toString(), ConnectorSpecification::class.java)

    validateSource(actorDefinitionId, partialDefaultConfig, convertedSpec)

    val entity =
      EntityConfigTemplate(
        organizationId = organizationId.value,
        actorDefinitionId = actorDefinitionId.value,
        partialDefaultConfig = partialDefaultConfig,
        userConfigSpec = userConfigSpec,
      )

    val configTemplate =
      repository
        .save(
          entity,
        ).toConfigModel()

    val actorDefinition = sourceService.getStandardSourceDefinition(configTemplate.actorDefinitionId, false)

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplate,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }

  override fun updateTemplate(
    configTemplateId: UUID,
    organizationId: OrganizationId,
    partialDefaultConfig: JsonNode?,
    userConfigSpec: JsonNode?,
  ): ConfigTemplateWithActorDetails {
    val configTemplate = repository.findById(configTemplateId).orElseThrow().toConfigModel()

    if (configTemplate.organizationId != organizationId.value) {
      throw IllegalArgumentException("OrganizationId does not match")
    }

    val updatedConfigTemplate =
      if (partialDefaultConfig != null || userConfigSpec != null) {
        val finalPartialDefaultConfig = partialDefaultConfig ?: configTemplate.partialDefaultConfig
        val finalUserConfigSpec =
          if (userConfigSpec != null) {
            val configSpecJsonString = Jsons.serialize(userConfigSpec)
            Jsons.deserialize(configSpecJsonString, ConnectorSpecification::class.java)
          } else {
            val configSpecJsonString = Jsons.serialize(configTemplate.userConfigSpec)
            Jsons.deserialize(configSpecJsonString, ConnectorSpecification::class.java)
          }

        validateSource(ActorDefinitionId(configTemplate.actorDefinitionId), finalPartialDefaultConfig, finalUserConfigSpec)

        val entity =
          EntityConfigTemplate(
            id = configTemplate.id,
            organizationId = configTemplate.organizationId,
            actorDefinitionId = configTemplate.actorDefinitionId,
            partialDefaultConfig = finalPartialDefaultConfig,
            userConfigSpec =
              finalUserConfigSpec.let {
                objectMapper.valueToTree<JsonNode>(it)
              },
          )
        repository.update(entity).toConfigModel()
      } else {
        configTemplate
      }

    val actorDefinition = sourceService.getStandardSourceDefinition(updatedConfigTemplate.actorDefinitionId, false)

    return ConfigTemplateWithActorDetails(
      configTemplate = updatedConfigTemplate,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }

  private fun validateSource(
    actorDefinitionId: ActorDefinitionId,
    partialDefaultConfig: JsonNode,
    userConfigSpec: ConnectorSpecification,
  ) {
    val actorDefinition =
      actorDefinitionService
        .getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId.value)
        .orElseThrow { throw RuntimeException("ActorDefinition not found") }

    val spec = actorDefinition.spec
    val combinationOfDefaultConfigAndPartialUserConfigSpec =
      mergeDefaultConfigAndPartialUserConfigSpec(
        partialDefaultConfig,
        userConfigSpec,
      )

    validateCombinationOfDefaultConfigAndPartialUserConfigSpec(spec, combinationOfDefaultConfigAndPartialUserConfigSpec)
  }

  private fun validateCombinationOfDefaultConfigAndPartialUserConfigSpec(
    spec: ConnectorSpecification,
    combinationOfDefaultConfigAndPartialUserConfigSpec: JsonNode,
  ) {
    validator.ensure(spec.connectionSpecification, combinationOfDefaultConfigAndPartialUserConfigSpec)
  }

  private fun mergeDefaultConfigAndPartialUserConfigSpec(
    defaultConfig: JsonNode,
    partialUserConfigSpec: ConnectorSpecification,
  ): JsonNode {
    if (!defaultConfig.isObject) {
      throw IllegalArgumentException("Default config must be object")
    }

    val objectMapper = ObjectMapper()
    val result = objectMapper.createObjectNode()

    // Add all keys from defaultConfig
    defaultConfig
      .fields()
      .forEach { (key, value) ->
        result.set<JsonNode>(key, value)
      }

    val requiredFields: List<String> =
      partialUserConfigSpec
        .connectionSpecification
        .get("required")
        .map { it.asText() }
        .toList()

    for (field in requiredFields) {
      result.set<JsonNode>(
        field,
        fieldDescriptionToMockValue(
          partialUserConfigSpec.connectionSpecification.get("properties")?.get(field) ?: objectMapper.nullNode(),
        ),
      )
    }

    return result
  }

  private fun fieldDescriptionToMockValue(jsonNode: JsonNode): JsonNode {
    /*
    Checks the type of the JSON object and returns a mock value based on the type.
    This will be used to create a merged config and validate that it respects a JSON schema.
     */
    if (!jsonNode.isObject) {
      throw IllegalArgumentException("Expected a JSON object. got ${jsonNode.nodeType}")
    }

    val typeNode = jsonNode.get("type") ?: throw IllegalArgumentException("Missing 'type' field")
    return when (val type = typeNode.asText()) {
      "string" -> jsonNode.get("default") ?: JsonNodeFactory.instance.textNode("mock_string")
      "integer" -> jsonNode.get("default") ?: JsonNodeFactory.instance.numberNode(42)
      "number" -> jsonNode.get("default") ?: JsonNodeFactory.instance.numberNode(3.14)
      "boolean" -> jsonNode.get("default") ?: JsonNodeFactory.instance.booleanNode(true)
      "array" -> JsonNodeFactory.instance.arrayNode()

      "object" -> JsonNodeFactory.instance.objectNode()

      else -> throw IllegalArgumentException("Unsupported type: $type")
    }
  }
}
