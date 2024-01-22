package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import io.airbyte.commons.json.Jsons

class JsonNodeAdapter {
  @ToJson
  fun toJson(value: JsonNode): String {
    return Jsons.serialize(value)
  }

  @FromJson
  fun fromJson(value: String): JsonNode {
    return Jsons.deserialize(value)
  }
}
