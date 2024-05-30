/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import io.airbyte.commons.json.Jsons

/**
 * Converts any numbers found in the JSON to integer representations.
 * By default, Moshi maps a JSON number to a [Double] when the target
 * field type is [Any].  This is because the JSON specification supports
 * both floating point and integer numbers in a number field and Moshi
 * does not have enough type information available to determine what type
 * the number represents.
 */
@Suppress("UNCHECKED_CAST")
fun transformNumbersToInts(value: Map<String, Any>): Map<String, Any> {
  return value
    .map { e ->
      when (e.value) {
        is Number -> e.key to transformNumberValue(e.value)
        is Map.Entry<*, *> -> e.key to transformNumberValue(e.value)
        is Map<*, *> -> e.key to transformNumbersToInts(e.value as Map<String, Any>)
        is List<*> -> e.key to transformNumberValue(e.value)
        else -> e.key to e.value
      }
    }.toMap()
}

@Suppress("UNCHECKED_CAST")
fun transformNumberValue(value: Any): Any {
  return when (value) {
    is Number -> if (value.toDouble() % 1.0 == 0.0) value.toLong() else value
    is Map.Entry<*, *> -> transformNumberValue(value.value as Map.Entry<*, *>)
    is Map<*, *> -> transformNumbersToInts(value as Map<String, Any>)
    is List<*> -> value.map { v -> transformNumberValue(v!!) }.toList()
    else -> value
  }
}

/**
 * Custom Moshi adapter that handles the use of [JsonNode]
 * that is used throughout the OpenAPI specification.
 * <p />
 * This adapter ensures that strings can be deserialized into [JsonNode]
 * fields and that [JsonNode] fields in the model objects will be
 * serialized into String representations of the JSON data.
 */
class JsonNodeAdapter {
  @Suppress("UNCHECKED_CAST")
  @ToJson
  fun toJson(value: JsonNode?): Map<String, Any> {
    return value?.let {
      if (value is NullNode) emptyMap() else Jsons.`object`(value, Map::class.java) as Map<String, Any>
    } ?: emptyMap()
  }

  @FromJson
  fun fromJson(value: Map<String, Any>?): JsonNode {
    return value?.let { Jsons.jsonNode(transformNumberValue(value)) } ?: Jsons.jsonNode(mapOf<String, Any>())
  }
}
