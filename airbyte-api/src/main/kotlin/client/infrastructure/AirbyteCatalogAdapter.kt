/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.FromJson
import com.squareup.moshi.Moshi
import com.squareup.moshi.ToJson
import com.squareup.moshi.adapter
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.commons.json.Jsons
import org.openapitools.client.infrastructure.BigDecimalAdapter
import org.openapitools.client.infrastructure.BigIntegerAdapter
import org.openapitools.client.infrastructure.ByteArrayAdapter
import org.openapitools.client.infrastructure.LocalDateAdapter
import org.openapitools.client.infrastructure.LocalDateTimeAdapter
import org.openapitools.client.infrastructure.OffsetDateTimeAdapter
import org.openapitools.client.infrastructure.URIAdapter
import org.openapitools.client.infrastructure.UUIDAdapter

/**
 * Custom Moshi adapter to handle the [Any] type "jsonSchema" property of the
 * [AirbyteStream] field that is part of the
 * [AirbyteCatalog] generated API model class that is
 * really a [com.fasterxml.jackson.databind.JsonNode] object and needs
 * a custom adapter to handle the conversion to/from JSON.  This is necessary
 * because Moshi attempts to convert the [Any] type to a [LinkedHashMap],
 * which does not align with the actually provided value for the property.
 */
class AirbyteCatalogAdapter {
  /**
   * Copy of the adapter chain from [org.openapitools.client.infrastructure.Serializer] that
   * includes adapters required to convert an [AirbyteCatalog] to JSON with enums in lowercase.
   */
  private val moshi =
    Moshi.Builder()
      .add(DestinationSyncModeAdapter())
      .add(SyncModeAdapter())
      .add(JsonNodeAdapter())
      .add(OffsetDateTimeAdapter())
      .add(LocalDateTimeAdapter())
      .add(LocalDateAdapter())
      .add(UUIDAdapter())
      .add(ByteArrayAdapter())
      .add(URIAdapter())
      .add(KotlinJsonAdapterFactory())
      .add(BigDecimalAdapter())
      .add(BigIntegerAdapter())
      .build()

  @ToJson
  @OptIn(ExperimentalStdlibApi::class)
  fun toJson(value: AirbyteCatalog): Map<String, Any> {
    // Hack to ensure that enums in the catalog are written as lowercase to work with server-side models from Java
    val json = moshi.adapter<AirbyteCatalog>().toJson(value)
    return Jsons.deserialize(json, Map::class.java) as Map<String, Any>
  }

  @FromJson
  fun fromJson(value: Map<String, Any>): AirbyteCatalog {
    return Jsons.`object`(Jsons.jsonNode(transformNumbersToInts(value)), AirbyteCatalog::class.java)
  }
}
