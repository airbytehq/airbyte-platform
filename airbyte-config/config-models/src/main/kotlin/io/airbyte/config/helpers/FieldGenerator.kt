/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_INTEGER
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIMESTAMP_WITHOUT_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIMESTAMP_WITH_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIME_WITHOUT_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIME_WITH_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.FORMAT
import io.airbyte.config.JsonsSchemaConstants.FORMAT_DATE
import io.airbyte.config.JsonsSchemaConstants.FORMAT_DATE_TIME
import io.airbyte.config.JsonsSchemaConstants.FORMAT_TIME
import io.airbyte.config.JsonsSchemaConstants.PROPERTIES
import io.airbyte.config.JsonsSchemaConstants.REQUIRED
import io.airbyte.config.JsonsSchemaConstants.TYPE
import io.airbyte.config.JsonsSchemaConstants.TYPE_ARRAY
import io.airbyte.config.JsonsSchemaConstants.TYPE_BOOLEAN
import io.airbyte.config.JsonsSchemaConstants.TYPE_INTEGER
import io.airbyte.config.JsonsSchemaConstants.TYPE_NUMBER
import io.airbyte.config.JsonsSchemaConstants.TYPE_OBJECT
import io.airbyte.config.JsonsSchemaConstants.TYPE_ONE_OF
import io.airbyte.config.JsonsSchemaConstants.TYPE_STRING
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

val log = KotlinLogging.logger {}

@Singleton
class FieldGenerator {
  fun getFieldsFromSchema(schema: JsonNode): List<Field> {
    val required = schema.get(REQUIRED)
    val requiredProperties: List<String> =
      if (required != null && required.isArray) {
        removeNullFromArray(required as ArrayNode)
      } else {
        listOf()
      }

    val properties = schema.get(PROPERTIES)
    if (properties != null && properties.isObject) {
      val arrProperties = properties as ObjectNode
      return arrProperties.properties().map { (key, value) ->
        Field(
          name = key,
          type = getFieldTypeFromNode(value),
          required = requiredProperties.contains(key),
        )
      }
    } else {
      return listOf()
    }
  }

  internal fun getFieldTypeFromNode(node: JsonNode): FieldType {
    try {
      if (node.has(TYPE_ONE_OF) || node.has("anyOf")) {
        return FieldType.MULTI
      }

      val type = node.get(TYPE) ?: return FieldType.UNKNOWN

      return if (type.isArray) {
        val curatedArray = removeNullFromArray(type as ArrayNode)

        if (curatedArray.size == 1) {
          getFieldTypeFromSchemaType(curatedArray[0], node)
        } else {
          FieldType.MULTI
        }
      } else {
        getFieldTypeFromSchemaType(type.asText(), node)
      }
    } catch (e: Exception) {
      log.warn { "Error getting field type from node: $node" }
      return FieldType.UNKNOWN
    }
  }

  internal fun getFieldTypeFromSchemaType(
    schemaType: String,
    node: JsonNode,
  ): FieldType {
    when (schemaType) {
      TYPE_BOOLEAN -> {
        return FieldType.BOOLEAN
      }
      TYPE_INTEGER -> {
        return FieldType.INTEGER
      }
      TYPE_NUMBER -> {
        val airbyteType = if (node.has(AIRBYTE_TYPE)) node.get(AIRBYTE_TYPE).asText() else null
        return if (airbyteType == AIRBYTE_TYPE_INTEGER) FieldType.INTEGER else FieldType.NUMBER
      }
      TYPE_OBJECT -> {
        return FieldType.OBJECT
      }
      TYPE_ARRAY -> {
        return FieldType.ARRAY
      }
      TYPE_STRING -> {
        val format = if (node.has(FORMAT)) node.get(FORMAT).asText() else null
        val airbyteType = if (node.has(AIRBYTE_TYPE)) node.get(AIRBYTE_TYPE).asText() else null
        if (format == null && airbyteType == null) {
          return FieldType.STRING
        } else if (format == FORMAT_DATE) {
          return FieldType.DATE
        } else if (format == FORMAT_DATE_TIME && airbyteType == null) {
          return FieldType.TIMESTAMP_WITH_TIMEZONE
        } else if (format == FORMAT_DATE_TIME && airbyteType == AIRBYTE_TYPE_TIMESTAMP_WITHOUT_TIMEZONE) {
          return FieldType.TIMESTAMP_WITHOUT_TIMEZONE
        } else if (format == FORMAT_DATE_TIME && airbyteType == AIRBYTE_TYPE_TIMESTAMP_WITH_TIMEZONE) {
          return FieldType.TIMESTAMP_WITH_TIMEZONE
        } else if (format == FORMAT_TIME && airbyteType == AIRBYTE_TYPE_TIME_WITHOUT_TIMEZONE) {
          return FieldType.TIME_WITHOUT_TIMEZONE
        } else if (format == FORMAT_TIME && airbyteType == AIRBYTE_TYPE_TIME_WITH_TIMEZONE) {
          return FieldType.TIME_WITH_TIMEZONE
        } else {
          log.warn { "Unknown string schema: $node" }
          return FieldType.UNKNOWN
        }
      }
      else -> {
        log.warn { "Unknown schema: $node" }
        return FieldType.UNKNOWN
      }
    }
  }

  internal fun removeNullFromArray(types: ArrayNode): List<String> =
    types
      .map { it.asText() }
      .filterNot { it == "null" }
}
