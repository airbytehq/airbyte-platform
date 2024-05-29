/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.openapitools.client.infrastructure.Serializer

internal class SyncModeTest {
  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeSyncModes() {
    val json = MoreResources.readResource("json/requests/source_discover_schema_write_request.json").trimIndent()
    val adapter = Serializer.moshi.adapter<SourceDiscoverSchemaWriteRequestBody>()
    val result = assertDoesNotThrow { adapter.fromJson(json) }
    assertNotNull(result)
    assertEquals(
      json,
      adapter.indent("  ").toJson(result)
        // Moshi's formatting does not jibe with our code style formatting
        .replace(
          oldValue =
            "\"supportedSyncModes\": [\n" +
              "            \"full_refresh\",\n" +
              "            \"incremental\"\n" +
              "          ],",
          newValue = "\"supportedSyncModes\": [\"full_refresh\", \"incremental\"],",
        ),
    )
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun test() {
    val config =
      AirbyteStreamConfiguration(
        syncMode = SyncMode.INCREMENTAL,
        destinationSyncMode = DestinationSyncMode.OVERWRITE,
      )
    val adapter = Serializer.moshi.adapter<AirbyteStreamConfiguration>()
    println(adapter.toJson(config))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun test2() {
    val stream =
      AirbyteStream(
        name = "test",
        supportedSyncModes = listOf(SyncMode.INCREMENTAL, SyncMode.FULL_REFRESH),
      )
    val adapter = Serializer.moshi.adapter<AirbyteStream>()
    println(adapter.toJson(stream))
  }

  @Test
  internal fun testSyncModeToJson() {
    val adapter = SyncModeAdapter()
    assertEquals(SyncMode.INCREMENTAL.name.lowercase(), adapter.toJson(SyncMode.INCREMENTAL))
  }

  @Test
  internal fun testDestinationSyncModeToJson() {
    val adapter = DestinationSyncModeAdapter()
    assertEquals(DestinationSyncMode.OVERWRITE.name.lowercase(), adapter.toJson(DestinationSyncMode.OVERWRITE))
  }

  @Test
  internal fun testSyncModeFromJson() {
    val adapter = SyncModeAdapter()
    assertEquals(SyncMode.INCREMENTAL, adapter.fromJson(SyncMode.INCREMENTAL.name.lowercase()))
    assertEquals(SyncMode.INCREMENTAL, adapter.fromJson(SyncMode.INCREMENTAL.name.uppercase()))
  }

  @Test
  internal fun testDestinationSyncModeFromJson() {
    val adapter = DestinationSyncModeAdapter()
    assertEquals(DestinationSyncMode.OVERWRITE, adapter.fromJson(DestinationSyncMode.OVERWRITE.name.lowercase()))
    assertEquals(DestinationSyncMode.OVERWRITE, adapter.fromJson(DestinationSyncMode.OVERWRITE.name.uppercase()))
  }
}
