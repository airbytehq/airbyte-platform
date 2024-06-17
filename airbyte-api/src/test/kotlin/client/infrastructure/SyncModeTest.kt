/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SyncMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer

internal class SyncModeTest {
  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeAirbyteStreamConfiguration() {
    val config =
      AirbyteStreamConfiguration(
        syncMode = SyncMode.INCREMENTAL,
        destinationSyncMode = DestinationSyncMode.OVERWRITE,
      )
    val adapter = Serializer.moshi.adapter<AirbyteStreamConfiguration>()
    val serializationResult = adapter.toJson(config)
    val deserializationResult = adapter.fromJson(serializationResult)
    assertEquals(config, deserializationResult)
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeAirbyteStream() {
    val stream =
      AirbyteStream(
        name = "test",
        supportedSyncModes = listOf(SyncMode.INCREMENTAL, SyncMode.FULL_REFRESH),
      )
    val adapter = Serializer.moshi.adapter<AirbyteStream>()
    val serializationResult = adapter.toJson(stream)
    val deserializationResult = adapter.fromJson(serializationResult)
    assertEquals(stream, deserializationResult)
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
