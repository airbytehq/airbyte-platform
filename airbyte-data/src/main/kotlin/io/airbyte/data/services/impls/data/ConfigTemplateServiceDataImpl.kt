/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.repositories.ConfigTemplateRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.data.mappers.EntityConfigTemplate
import io.airbyte.data.services.impls.data.mappers.objectMapper
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonMergingHelper
import io.airbyte.validation.json.JsonSchemaValidator
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
open class ConfigTemplateServiceDataImpl(
  private val repository: ConfigTemplateRepository,
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceService: SourceService,
  private val oAuthService: OAuthService,
  private val validator: JsonSchemaValidator,
) : ConfigTemplateService {
  private val jsonMergingHelper = JsonMergingHelper()
  private val schemaDefaultValueHelper = SchemaDefaultValueHelper()

  override fun getConfigTemplate(configTemplateId: UUID): ConfigTemplateWithActorDetails {
    val template = repository.findById(configTemplateId).orElseThrow()

    var configTemplate = template.toConfigModel()

    val actorDefinition = sourceService.getStandardSourceDefinition(configTemplate.actorDefinitionId, false)

    val actorDefinitionSpec = getConnectorSpecification(ActorDefinitionId(configTemplate.actorDefinitionId))

    configTemplate =
      configTemplate.copy(
        advancedAuth = actorDefinitionSpec.advancedAuth,
      )

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplate,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }

  override fun getConfigTemplate(
    configTemplateId: UUID,
    workspaceId: UUID,
  ): ConfigTemplateWithActorDetails {
    val template = repository.findById(configTemplateId).orElseThrow()

    var configTemplate = template.toConfigModel()

    val actorDefinitionSpec = getConnectorSpecification(ActorDefinitionId(configTemplate.actorDefinitionId))
    val actorDefinition = sourceService.getStandardSourceDefinition(configTemplate.actorDefinitionId, false)

    val isAdvancedAuthGlobalCredentialsAvailable =
      if (actorDefinitionSpec.advancedAuth != null) {
        val sourceOAuthParameter: Optional<SourceOAuthParameter> =
          oAuthService.getSourceOAuthParameterOptional(workspaceId, actorDefinition.sourceDefinitionId)
        sourceOAuthParameter.isPresent
      } else {
        false
      }

    configTemplate =
      configTemplate.copy(
        advancedAuth = actorDefinitionSpec.advancedAuth,
        advancedAuthGlobalCredentialsAvailable = isAdvancedAuthGlobalCredentialsAvailable,
      )

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplate,
      actorName = actorDefinition.name,
      actorIcon = actorDefinition.iconUrl,
    )
  }

  override fun listConfigTemplatesForOrganization(organizationId: OrganizationId): List<ConfigTemplateWithActorDetails> {
    // Get source definitions
    val actorDefinitions = sourceService.listStandardSourceDefinitions(false)
    if (actorDefinitions.isEmpty()) {
      return emptyList()
    }

    val actorDefinitionIds = actorDefinitions.map { it.sourceDefinitionId }
    val actorDefinitionsById = actorDefinitions.associateBy { it.sourceDefinitionId }

    // Get all templates (both custom and default)
    val defaultTemplates =
      repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(actorDefinitionIds).groupBy {
        it.actorDefinitionId
      }
    val customTemplates = repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(organizationId.value, actorDefinitionIds)

    // Group templates by actor definition ID

    val selectedTemplates = mutableListOf<EntityConfigTemplate>()

// Add all custom templates
    selectedTemplates.addAll(customTemplates)

// Add default templates that don't have a custom template for the same actor definition
    actorDefinitionIds.forEach { actorDefinitionId ->
      val hasCustomTemplate = customTemplates.any { it.actorDefinitionId == actorDefinitionId }

      // If there's no custom template for this actor definition, add the default one
      if (!hasCustomTemplate) {
        defaultTemplates[actorDefinitionId]?.find { it.organizationId == null }?.let {
          selectedTemplates.add(it)
        }
      }
    }

    // Create and return template results
    return createTemplateResults(selectedTemplates, actorDefinitionsById)
  }

  private fun createTemplateResults(
    templates: List<EntityConfigTemplate>,
    actorDefinitionsById: Map<UUID, StandardSourceDefinition>,
  ): List<ConfigTemplateWithActorDetails> {
    return templates.mapNotNull { template ->
      val actorDef = actorDefinitionsById[template.actorDefinitionId] ?: return@mapNotNull null

      ConfigTemplateWithActorDetails(
        configTemplate = template.toConfigModel(),
        actorName = actorDef.name,
        actorIcon = actorDef.iconUrl,
      )
    }
  }

  override fun createTemplate(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    partialDefaultConfig: JsonNode,
    userConfigSpec: JsonNode?,
  ): ConfigTemplateWithActorDetails {
    val actorDefinitionSpec = getConnectorSpecification(actorDefinitionId)

    val userSpec =
      userConfigSpec ?: inferPartialUserSpec(actorDefinitionSpec, partialDefaultConfig)

    val convertedSpec = Jsons.deserialize(userSpec.toString(), ConnectorSpecification::class.java)

    validateSource(actorDefinitionSpec, partialDefaultConfig, convertedSpec)

    val entity =
      EntityConfigTemplate(
        organizationId = organizationId.value,
        actorDefinitionId = actorDefinitionId.value,
        partialDefaultConfig = partialDefaultConfig,
        userConfigSpec = userSpec,
      )

    val configTemplate =
      repository
        .save(
          entity,
        ).toConfigModel()

    val standardActorDefinition = sourceService.getStandardSourceDefinition(configTemplate.actorDefinitionId, false)

    return ConfigTemplateWithActorDetails(
      configTemplate = configTemplate,
      actorName = standardActorDefinition.name,
      actorIcon = standardActorDefinition.iconUrl,
    )
  }

  private fun getConnectorSpecification(actorDefinitionId: ActorDefinitionId): ConnectorSpecification {
    val actorDefinition =
      actorDefinitionService
        .getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId.value)
        .orElseThrow { throw RuntimeException("ActorDefinition not found") }
    val actorDefinitionSpec = actorDefinition.spec
    return actorDefinitionSpec
  }

  private fun getOauthRequiredFields(actorDefinitionSpec: ConnectorSpecification): List<String> {
    if (actorDefinitionSpec.advancedAuth == null) {
      return emptyList()
    }

    val completeOauthOutputProperties =
      actorDefinitionSpec.advancedAuth.oauthConfigSpecification.completeOauthOutputSpecification
        .get("properties") as ObjectNode

    val completeOauthServerProperties =
      actorDefinitionSpec.advancedAuth.oauthConfigSpecification.completeOauthServerInputSpecification
        .get("properties") as ObjectNode

    val completeOauthServerOutputProperties =
      actorDefinitionSpec.advancedAuth.oauthConfigSpecification.completeOauthServerOutputSpecification
        .get("properties") as ObjectNode

    // List to collect all the required fields for OAuth
    val oauthRequiredFields = mutableListOf<String>()

// Process completeOauthOutputProperties
    completeOauthOutputProperties.fields().forEach { (_, propertyObj) ->
      // Check if the property object has "path_in_connector_config"
      if (propertyObj.has("path_in_connector_config") && propertyObj.get("path_in_connector_config").isArray) {
        val pathArray = propertyObj.get("path_in_connector_config") as ArrayNode
        // Add each element in the array to the list
        (0 until pathArray.size()).forEach { i ->
          oauthRequiredFields.add(pathArray.get(i).asText())
        }
      }
    }

// Process completeOauthServerProperties
    completeOauthServerProperties.fields().forEach { (_, propertyObj) ->
      // Check if the property object has "path_in_connector_config"
      if (propertyObj.has("path_in_connector_config") && propertyObj.get("path_in_connector_config").isArray) {
        val pathArray = propertyObj.get("path_in_connector_config") as ArrayNode
        // Add each element in the array to the list
        (0 until pathArray.size()).forEach { i ->
          oauthRequiredFields.add(pathArray.get(i).asText())
        }
      }
    }
    completeOauthServerOutputProperties.fields().forEach { (_, propertyObj) ->
      // Check if the property object has "path_in_connector_config"
      if (propertyObj.has("path_in_connector_config") && propertyObj.get("path_in_connector_config").isArray) {
        val pathArray = propertyObj.get("path_in_connector_config") as ArrayNode
        // Add each element in the array to the list
        (0 until pathArray.size()).forEach { i ->
          oauthRequiredFields.add(pathArray.get(i).asText())
        }
      }
    }

    return oauthRequiredFields.toList()
  }

  private fun inferPartialUserSpec(
    actorDefinitionSpec: ConnectorSpecification,
    partialDefaultConfig: JsonNode,
  ): JsonNode {
    val inferredSpec: JsonNode = actorDefinitionSpec.connectionSpecification.deepCopy()

    val required = inferredSpec.get("required") as ArrayNode
    val requiredForOauth = getOauthRequiredFields(actorDefinitionSpec)

    required.removeAll { partialDefaultConfig.has(it.asText()) && !requiredForOauth.contains(it.asText()) }

    val properties = inferredSpec.get("properties") as ObjectNode
    val requiredProperties = required.map { it.asText() }
    val allFields: Set<String> = properties.fieldNames().asSequence().toSet()
    for (f in allFields) {
      if (!requiredProperties.contains(f) && !requiredForOauth.contains(f)) {
        properties.remove(f)
      }
    }
    val connectionSpec = objectMapper.valueToTree<ObjectNode>(actorDefinitionSpec).deepCopy()

    (inferredSpec as ObjectNode).put("type", "object")

    connectionSpec.set<ObjectNode>("connectionSpecification", inferredSpec)

    return connectionSpec
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
        val finalPartialDefaultConfig = partialDefaultConfig ?: (configTemplate.partialDefaultConfig)
        val finalUserConfigSpec =
          if (userConfigSpec != null) {
            val configSpecJsonString = Jsons.serialize(userConfigSpec)
            Jsons.deserialize(configSpecJsonString, ConnectorSpecification::class.java)
          } else {
            val configSpecJsonString = Jsons.serialize(configTemplate.userConfigSpec)
            Jsons.deserialize(configSpecJsonString, ConnectorSpecification::class.java)
          }

        validateSource(getConnectorSpecification(ActorDefinitionId(configTemplate.actorDefinitionId)), finalPartialDefaultConfig, finalUserConfigSpec)

        val entity =
          EntityConfigTemplate(
            id = configTemplate.id,
            organizationId = configTemplate.organizationId,
            actorDefinitionId = configTemplate.actorDefinitionId,
            partialDefaultConfig = finalPartialDefaultConfig,
            userConfigSpec =
              finalUserConfigSpec.let {
                objectMapper.valueToTree(it)
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
    connectorSpecification: ConnectorSpecification,
    partialDefaultConfig: JsonNode,
    userConfigSpec: ConnectorSpecification,
  ) {
    val combinationOfDefaultConfigAndPartialUserConfigSpec =
      mergeDefaultConfigAndPartialUserConfigSpec(
        partialDefaultConfig,
        userConfigSpec,
      )

    validateCombinationOfDefaultConfigAndPartialUserConfigSpec(connectorSpecification, combinationOfDefaultConfigAndPartialUserConfigSpec)
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

    val mockValuesFromUserConfigSpec = schemaDefaultValueHelper.createDefaultValuesFromSchema(partialUserConfigSpec.connectionSpecification)

    return jsonMergingHelper.combineProperties(
      defaultConfig,
      mockValuesFromUserConfigSpec,
    )
  }
}

class SchemaDefaultValueHelper {
  fun createDefaultValuesFromSchema(schema: JsonNode): JsonNode =
    when (schema.get("type")?.asText()) {
      "object" -> buildObjectDefault(schema)
      "string" -> buildStringDefault(schema)
      "array" -> buildArrayDefault(schema)
      "integer" -> JsonNodeFactory.instance.numberNode(42)
      "number" -> JsonNodeFactory.instance.numberNode(3.14)
      "boolean" -> JsonNodeFactory.instance.booleanNode(true)
      else -> NullNode.instance
    }

  private fun buildArrayDefault(schema: JsonNode): ArrayNode {
    val nodeFactory = JsonNodeFactory.instance
    val result = nodeFactory.arrayNode()
    // If no template for the array contents is provided, just return a mock string
    val items = schema.get("items") ?: return result.add(buildStringDefault(nodeFactory.textNode("mock_string")))
    return items
      .takeIf { it.isObject }
      ?.let { result.add(createDefaultValuesFromSchema(it)) }
      ?: result.add(buildStringDefault(nodeFactory.textNode("mock_string")))
  }

  private fun buildObjectDefault(schema: JsonNode): JsonNode {
    val nodeFactory = JsonNodeFactory.instance
    val result = nodeFactory.objectNode()

    // If it's a oneOf, return the first schema
    if (schema.has("oneOf")) {
      val oneOf = schema.get("oneOf")
      if (oneOf.isArray && oneOf.size() > 0) {
        val firstSchema = oneOf[0]
        if (firstSchema.has("type")) {
          return createDefaultValuesFromSchema(firstSchema)
        } else {
          // Some of our connector schemas have objects in the oneOf array that don't have a "type": "object"
          return buildObjectDefault(oneOf[0])
        }
      }
    }

    val properties = schema.get("properties") ?: return result
    // only include required props
    val requiredFields =
      schema
        .get("required")
        ?.map { it.asText() }
        ?.toSet()
        ?: emptySet()

    for ((propName, propSchema) in properties.fields()) {
      if (propName in requiredFields) {
        result.set<JsonNode>(
          propName,
          createDefaultValuesFromSchema(propSchema),
        )
      }
    }
    return result
  }

  private fun buildStringDefault(schema: JsonNode): TextNode {
    // 1. check the “default” field
    schema.get("default")?.asText()?.let { return TextNode(it) }
    // 2. fall back to the pattern_descriptor
    schema.get("pattern_descriptor")?.asText()?.let { return TextNode(it) }
    // 3. fall back to the first example in the examples array
    schema
      .get("examples")
      ?.takeIf { it.isArray && it.size() > 0 }
      ?.get(0)
      ?.asText()
      ?.let { return TextNode(it) }
    // 4. explicit format-based values
    return when (schema.get("format")?.asText()) {
      "date" -> TextNode("1970-01-01")
      "date-time" -> TextNode("1970-01-01T00:00:00Z")
      // If all else fails, use a mock string
      else -> TextNode("mock_string")
    }
  }
}
