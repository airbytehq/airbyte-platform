/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.victools.jsonschema.generator.CustomPropertyDefinition
import com.github.victools.jsonschema.generator.CustomPropertyDefinitionProvider
import com.github.victools.jsonschema.generator.FieldScope
import com.github.victools.jsonschema.generator.OptionPreset
import com.github.victools.jsonschema.generator.SchemaGenerationContext
import com.github.victools.jsonschema.generator.SchemaGenerator
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder
import com.github.victools.jsonschema.generator.SchemaVersion
import com.github.victools.jsonschema.generator.TypeAttributeOverrideV2
import com.github.victools.jsonschema.generator.TypeScope
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.JsonsSchemaConstants
import io.airbyte.config.mapper.configs.NotNull
import io.airbyte.config.mapper.configs.SchemaConstant
import io.airbyte.config.mapper.configs.SchemaDefault
import io.airbyte.config.mapper.configs.SchemaDescription
import io.airbyte.config.mapper.configs.SchemaExamples
import io.airbyte.config.mapper.configs.SchemaFormat
import io.airbyte.config.mapper.configs.SchemaTitle
import jakarta.inject.Singleton

@Singleton
class SimpleJsonSchemaGeneratorFromSpec {
  fun <T> generateJsonSchema(classType: Class<T>): JsonNode {
    val configBuilder =
      SchemaGeneratorConfigBuilder(
        SchemaVersion.DRAFT_7,
        OptionPreset.PLAIN_JSON,
      )

    configBuilder
      .forFields()
      .withRequiredCheck { field ->
        field.getAnnotation(NotNull::class.java) != null
      }.withCustomDefinitionProvider(SimplePropertyDefProvider())

    configBuilder
      .forTypesInGeneral()
      .withTypeAttributeOverride(JsonSubTypesOverride())

    val generator = SchemaGenerator(configBuilder.build())
    return generator.generateSchema(classType)
  }
}

class JsonSubTypesOverride : TypeAttributeOverrideV2 {
  override fun overrideTypeAttributes(
    node: ObjectNode,
    typeScope: TypeScope,
    schemaGenerationContext: SchemaGenerationContext,
  ) {
    if (typeScope.type.erasedType == AirbyteSecret::class.java) {
      node.apply {
        removeAll()
        set<JsonNode>("airbyte_secret", schemaGenerationContext.generatorConfig.objectMapper.convertValue(true, JsonNode::class.java))
      }
      return
    }

    val jsonSubTypesAnnotation = typeScope.type.erasedType.getAnnotation(JsonSubTypes::class.java)
    if (jsonSubTypesAnnotation != null) {
      val generator = SchemaGenerator(schemaGenerationContext.generatorConfig, schemaGenerationContext.typeContext)
      val subTypeSchemas = jsonSubTypesAnnotation.value.map { generator.generateSchema(it.value.java) }
      node.apply {
        removeAll()
        set<JsonNode>(
          JsonsSchemaConstants.TYPE_ONE_OF,
          schemaGenerationContext.generatorConfig.objectMapper
            .createArrayNode()
            .addAll(subTypeSchemas),
        )
      }
    }
  }
}

class SimplePropertyDefProvider : CustomPropertyDefinitionProvider<FieldScope> {
  override fun provideCustomSchemaDefinition(
    fieldScope: FieldScope?,
    schemaGenerationContext: SchemaGenerationContext?,
  ): CustomPropertyDefinition {
    val objectMapper = ObjectMapper()
    val objectNode: ObjectNode = objectMapper.createObjectNode()
    if (fieldScope == null || schemaGenerationContext == null) {
      return CustomPropertyDefinition(objectNode)
    }

    val isEnum = fieldScope.type.erasedType.isEnum

    val node = if (isEnum) objectNode else schemaGenerationContext.createDefinition(fieldScope.type)!!

    if (isEnum) {
      setEnum(fieldScope.type.erasedType.enumConstants, objectMapper, node)
    }

    setTitle(fieldScope, node)
    setDescription(fieldScope, node)
    setConstant(fieldScope, node)
    setDefault(fieldScope, node)
    setFormat(fieldScope, node)
    setExamples(fieldScope, objectMapper, node)
    return CustomPropertyDefinition(node)
  }

  private fun setFormat(
    fieldScope: FieldScope,
    objectNode: ObjectNode,
  ) {
    val format = fieldScope.getAnnotation(SchemaFormat::class.java)?.format ?: return
    objectNode.put("format", format)
  }

  private fun setConstant(
    fieldScope: FieldScope,
    objectNode: ObjectNode,
  ) {
    val constant = fieldScope.getAnnotation(SchemaConstant::class.java)?.constant ?: return
    objectNode.put("const", constant)
  }

  private fun setDefault(
    fieldScope: FieldScope,
    objectNode: ObjectNode,
  ) {
    val defaultValue = fieldScope.getAnnotation(SchemaDefault::class.java)?.defaultValue ?: return
    objectNode.put("default", defaultValue)
  }

  private fun setDescription(
    fieldScope: FieldScope,
    objectNode: ObjectNode,
  ) {
    val description = fieldScope.getAnnotation(SchemaDescription::class.java)?.description ?: return
    objectNode.put("description", description)
  }

  private fun setTitle(
    fieldScope: FieldScope,
    objectNode: ObjectNode,
  ) {
    val title = fieldScope.getAnnotation(SchemaTitle::class.java)?.title ?: return
    objectNode.put("title", title)
  }

  private fun setEnum(
    enumConstants: Array<out Any>,
    objectMapper: ObjectMapper,
    objectNode: ObjectNode,
  ) {
    val map = enumConstants.map { it.toString() }
    val arrayNode: ArrayNode = objectMapper.createArrayNode()
    for (value in map) {
      arrayNode.add(value)
    }
    objectNode.put("type", "string")
    objectNode.set<JsonNode>("enum", arrayNode)
  }

  private fun setExamples(
    fieldScope: FieldScope,
    objectMapper: ObjectMapper,
    objectNode: ObjectNode,
  ) {
    val examples = fieldScope.getAnnotation(SchemaExamples::class.java)?.examples ?: return

    val arrayNode: ArrayNode = objectMapper.createArrayNode()
    for (value in examples) {
      arrayNode.add(value)
    }
    objectNode.set<JsonNode>("examples", arrayNode)
  }
}
