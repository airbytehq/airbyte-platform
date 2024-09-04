package io.airbyte.config.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import jakarta.inject.Singleton

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
    if (node.has("oneOf")) {
      return FieldType.MULTI
    }
    val type = node.get("type")
    if (type.isArray) {
      val curatedArray = removeNullFromArray(type as ArrayNode)

      if (curatedArray.size == 1) {
        return getFieldTypeFromSchemaType(curatedArray[0], node)
      } else {
        return FieldType.MULTI
      }
    } else {
      return getFieldTypeFromSchemaType(type.asText(), node)
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
          throw IllegalStateException("Unexpected format: $format and airbyteType: $airbyteType")
        }
      }
      else -> {
        throw IllegalStateException("Unexpected schema type: $schemaType")
      }
    }
  }

  internal fun removeNullFromArray(types: ArrayNode): List<String> {
    return types.map { it.asText() }
      .filterNot { it == "null" }
  }
}
