/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.ToJson
import io.airbyte.commons.json.Jsons
import org.openapitools.client.infrastructure.Serializer

/**
 * Custom Moshi adapter to handle schema2pojo generated configuration model objects.  These
 * objects are generated with Jackson annotations that control JSON deserialization.  As a result,
 * a custom adapter is required to ensure that when these objects are serialized correctly as part of a request
 * and those annotations are honored.
 */
class AirbyteGeneratedConfigAdapter {
  @ToJson
  fun toJson(value: Any): Any {
    /*
     * If the value is a generated Airbyte config object, use Jackson for deserialization.
     * The generated config objects are generated with Jackson annotations, so we need to
     * use Jackson to ensure that the proper directives are used when creating JSON.
     */
    return if (value.javaClass.packageName == "io.airbyte.config") {
      return Jsons.deserialize(Jsons.serialize(value), Map::class.java)
    } else {
      return when (value) {
        is Array<*> -> Jsons.deserialize(Serializer.moshi.adapter(Array::class.java).toJson(value), Array::class.java)
        is List<*> -> Jsons.deserialize(Serializer.moshi.adapter(List::class.java).toJson(value), List::class.java)
        is Map<*, *> -> Jsons.deserialize(Serializer.moshi.adapter(Map::class.java).toJson(value), Map::class.java)
        is String -> value
        is Number -> value
        is Boolean -> value
        else -> Jsons.deserialize(Serializer.moshi.adapter(value.javaClass).toJson(value), Map::class.java)
      }
    }
  }
}
