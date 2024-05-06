/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import io.airbyte.commons.json.Jsons

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
  fun toJson(value: JsonNode): Map<String, Any> {
    return Jsons.`object`(value, Map::class.java) as Map<String, Any>
  }

  @FromJson
  fun fromJson(value: Map<String, Any>): JsonNode {
    return Jsons.jsonNode(value)
  }
}
