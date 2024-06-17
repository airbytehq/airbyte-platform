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
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody
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
 * Provides an explicit adapter for SourceDiscoverSchemaWriteRequestBody which contains the catalog.
 * This ensures we have quick and backwards—compatible serialization performance by using Jackson. Without
 * this a less performant adapter (namely, AirbyteGeneratedConfigAdapter) may be chosen which can cause
 * timeouts and DISCOVER failures.
 */
class SourceDiscoverSchemaWriteRequestBodyAdapter {
  /**
   * Copy of the adapter chain from [org.openapitools.client.infrastructure.Serializer] that
   * includes adapters required to convert an [AirbyteCatalog] to JSON with enums in lowercase.
   */
  private val moshi =
    Moshi.Builder()
      .add(AirbyteCatalogAdapter())
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
  @Suppress("UNCHECKED_CAST")
  fun toJson(value: SourceDiscoverSchemaWriteRequestBody): Map<String, Any> {
    // Hack to ensure that enums in the catalog are written as lowercase to work with server-side models from Java
    val json = moshi.adapter<SourceDiscoverSchemaWriteRequestBody>().toJson(value)
    return Jsons.deserialize(json, Map::class.java) as Map<String, Any>
  }

  @FromJson
  fun fromJson(value: Map<String, Any>): SourceDiscoverSchemaWriteRequestBody {
    return Jsons.`object`(Jsons.jsonNode(value), SourceDiscoverSchemaWriteRequestBody::class.java)
  }
}
