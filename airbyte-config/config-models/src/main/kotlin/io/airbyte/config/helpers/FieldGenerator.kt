package io.airbyte.config.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

val log = KotlinLogging.logger {}

@Singleton
class FieldGenerator {
  fun getFieldsFromSchema(schema: JsonNode): List<Field> {
    val properties = schema.get("properties")
    if (properties.isObject) {
      val arrProperties = properties as ObjectNode
      return arrProperties.properties().map { (key, value) ->
        Field(
          name = key,
          type = getFieldTypeFromNode(value),
        )
      }
    } else {
      return listOf()
    }
  }

  internal fun getFieldTypeFromNode(node: JsonNode): FieldType {
    try {
      if (node.has("oneOf") || node.has("anyOf")) {
        return FieldType.MULTI
      }

      val type = node.get("type")

      if (type == null) {
        return FieldType.UNKNOWN
      }

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
      "boolean" -> {
        return FieldType.BOOLEAN
      }
      "integer" -> {
        return FieldType.INTEGER
      }
      "number" -> {
        val airbyteType = if (node.has("airbyte_type")) node.get("airbyte_type").asText() else null
        return if (airbyteType == "integer") FieldType.INTEGER else FieldType.NUMBER
      }
      "boolean" -> {
        return FieldType.BOOLEAN
      }
      "object" -> {
        return FieldType.OBJECT
      }
      "array" -> {
        return FieldType.ARRAY
      }
      "string" -> {
        val format = if (node.has("format")) node.get("format").asText() else null
        val airbyteType = if (node.has("airbyte_type")) node.get("airbyte_type").asText() else null
        if (format == null && airbyteType == null) {
          return FieldType.STRING
        } else if (format == "date") {
          return FieldType.DATE
        } else if (format == "date-time" && airbyteType == null) {
          return FieldType.TIMESTAMP_WITH_TIMEZONE
        } else if (format == "date-time" && airbyteType == "timestamp_without_timezone") {
          return FieldType.TIMESTAMP_WITHOUT_TIMEZONE
        } else if (format == "date-time" && airbyteType == "timestamp_with_timezone") {
          return FieldType.TIMESTAMP_WITH_TIMEZONE
        } else if (format == "time" && airbyteType == "time_without_timezone") {
          return FieldType.TIME_WITHOUT_TIMEZONE
        } else if (format == "time" && airbyteType == "time_with_timezone") {
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

  internal fun removeNullFromArray(types: ArrayNode): List<String> {
    return types.map { it.asText() }
      .filterNot { it == "null" }
  }
}
