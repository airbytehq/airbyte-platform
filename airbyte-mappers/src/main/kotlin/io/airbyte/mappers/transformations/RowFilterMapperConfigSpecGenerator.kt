/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.victools.jsonschema.generator.FieldScope
import com.github.victools.jsonschema.generator.Option
import com.github.victools.jsonschema.generator.OptionPreset
import com.github.victools.jsonschema.generator.SchemaGenerationContext
import com.github.victools.jsonschema.generator.SchemaGenerator
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder
import com.github.victools.jsonschema.generator.SchemaVersion
import com.github.victools.jsonschema.generator.TypeAttributeOverrideV2
import com.github.victools.jsonschema.generator.TypeScope
import io.airbyte.config.mapper.configs.EqualOperation
import io.airbyte.config.mapper.configs.NotNull
import io.airbyte.config.mapper.configs.NotOperation
import io.airbyte.config.mapper.configs.Operation
import io.airbyte.config.mapper.configs.SchemaConstant
import io.airbyte.config.mapper.configs.SchemaDefault
import io.airbyte.config.mapper.configs.SchemaDescription
import io.airbyte.config.mapper.configs.SchemaFormat
import io.airbyte.config.mapper.configs.SchemaTitle
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("RowFilterMapperConfigSpecGenerator")
class RowFilterMapperConfigSpecGenerator {
  private val objectMapper = ObjectMapper().registerKotlinModule()

  fun generateSchema(clazz: Class<*>): JsonNode {
    val config = buildSchemaGeneratorConfig()
    val generator = SchemaGenerator(config)
    return generator.generateSchema(clazz)
  }

  private fun buildSchemaGeneratorConfig(): SchemaGeneratorConfig {
    val configBuilder =
      SchemaGeneratorConfigBuilder(
        objectMapper,
        SchemaVersion.DRAFT_7,
        OptionPreset.PLAIN_JSON,
      ).apply {
        with(Option.DEFINITIONS_FOR_ALL_OBJECTS)
        with(Option.SCHEMA_VERSION_INDICATOR)
        with(Option.ALLOF_CLEANUP_AT_THE_END)
        with(Option.PLAIN_DEFINITION_KEYS)
        with(Option.FLATTENED_ENUMS)
      }

    configBuilder
      .forTypesInGeneral()
      .withTitleResolver {
        it.type.erasedType
          .getAnnotation(SchemaTitle::class.java)
          ?.title
      }.withDescriptionResolver {
        it.type.erasedType
          .getAnnotation(SchemaDescription::class.java)
          ?.description
      }.withTypeAttributeOverride(PolymorphicTypeResolver(objectMapper, configBuilder.build()))

    setFieldsConfigBuilder(configBuilder, false)

    return configBuilder.build()
  }

  companion object {
    private fun ObjectNode.setConstantAndFormat(member: FieldScope) {
      member.getAnnotation(SchemaConstant::class.java)?.let {
        put("const", it.constant)
      }
      member.getAnnotation(SchemaFormat::class.java)?.let {
        put("format", it.format)
      }
    }

    private fun setFieldsConfigBuilder(
      configBuilder: SchemaGeneratorConfigBuilder,
      shouldHandleConditions: Boolean,
    ) {
      configBuilder
        .forFields()
        .withRequiredCheck { it.getAnnotation(NotNull::class.java) != null }
        .withTitleResolver { it.getAnnotation(SchemaTitle::class.java)?.title }
        .withDescriptionResolver { it.getAnnotation(SchemaDescription::class.java)?.description }
        .withDefaultResolver { it.getAnnotation(SchemaDefault::class.java)?.defaultValue }
        .withInstanceAttributeOverride { objectNode, member, _ ->
          objectNode.setConstantAndFormat(member)
          if (shouldHandleConditions && member.name == "conditions") {
            if (member.type.erasedType == List::class.java) {
              objectNode.put("minItems", 1)
            } else {
              objectNode.removeAll()
              objectNode.put("\$ref", "#/definitions/Operation")
            }
          }
        }
    }
  }

  class PolymorphicTypeResolver(
    private val objectMapper: ObjectMapper,
    private val schemaGeneratorConfig: SchemaGeneratorConfig,
  ) : TypeAttributeOverrideV2 {
    override fun overrideTypeAttributes(
      node: ObjectNode?,
      typeScope: TypeScope?,
      context: SchemaGenerationContext?,
    ) {
      val erasedType = typeScope?.type?.erasedType
      if (erasedType == Operation::class.java) {
        val subTypes =
          listOf(
//            We want to hide the AND/OR operation from the end user for the first iteration, uncomment this when we want to expose them
//            generateSchemaForClass(AndOperation::class.java),
//            generateSchemaForClass(OrOperation::class.java),
            generateSchemaForClass(EqualOperation::class.java),
            generateSchemaForClass(NotOperation::class.java),
          )
        node?.apply {
          removeAll()
          set<JsonNode>("oneOf", objectMapper.createArrayNode().addAll(subTypes))
        }
      }
    }

    private fun generateSchemaForClass(clazz: Class<*>): JsonNode {
      val configBuilder =
        SchemaGeneratorConfigBuilder(
          objectMapper,
          SchemaVersion.DRAFT_7,
          OptionPreset.PLAIN_JSON,
        ).apply {
          with(Option.ALLOF_CLEANUP_AT_THE_END, Option.PLAIN_DEFINITION_KEYS)
        }

      setFieldsConfigBuilder(configBuilder, true)

      val generator = SchemaGenerator(configBuilder.build())
      return generator.generateSchema(clazz)
    }
  }
}
