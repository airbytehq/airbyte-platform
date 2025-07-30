/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.Resources
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.CatalogTransforms
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.List
import java.util.function.Function

internal class CatalogTransformsTest {
  @Test
  @Throws(IOException::class)
  fun testResetCatalogSyncModeReplacement() {
    val catalog = Resources.read("catalogs/simple_catalog.json")
    val configuredAirbyteCatalog =
      Jsons.`object`(
        Jsons.deserialize(catalog),
        ConfiguredAirbyteCatalog::class.java,
      )
    val streamDescriptor = StreamDescriptor().withName("pokemon")
    val streamsToReset = List.of(streamDescriptor)

    CatalogTransforms.updateCatalogForReset(streamsToReset, configuredAirbyteCatalog)

    Assertions.assertEquals(
      DestinationSyncMode.OVERWRITE,
      findStreamSyncMode(
        configuredAirbyteCatalog,
        streamDescriptor,
        ConfiguredAirbyteStream::destinationSyncMode,
      ),
    )
    Assertions.assertEquals(
      SyncMode.FULL_REFRESH,
      findStreamSyncMode(
        configuredAirbyteCatalog,
        streamDescriptor,
        ConfiguredAirbyteStream::syncMode,
      ),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testResetCatalogSyncModeReplacementMultipleStreams() {
    val catalog = Resources.read("catalogs/multiple_stream_catalog.json")
    val configuredAirbyteCatalog =
      Jsons.`object`(
        Jsons.deserialize(catalog),
        ConfiguredAirbyteCatalog::class.java,
      )
    val streamDescriptor = StreamDescriptor().withName("pokemon")
    val otherStreamDescriptor = StreamDescriptor().withName("other")
    val otherStreamDescriptor2 = StreamDescriptor().withName("other2").withNamespace("namespace")
    val streamsToReset = List.of(streamDescriptor)

    CatalogTransforms.updateCatalogForReset(streamsToReset, configuredAirbyteCatalog)

    Assertions.assertEquals(
      DestinationSyncMode.OVERWRITE,
      findStreamSyncMode(
        configuredAirbyteCatalog,
        streamDescriptor,
        ConfiguredAirbyteStream::destinationSyncMode,
      ),
    )
    Assertions.assertEquals(
      SyncMode.FULL_REFRESH,
      findStreamSyncMode(
        configuredAirbyteCatalog,
        streamDescriptor,
        ConfiguredAirbyteStream::syncMode,
      ),
    )
    Assertions.assertFalse(contains(configuredAirbyteCatalog, otherStreamDescriptor))
    Assertions.assertFalse(contains(configuredAirbyteCatalog, otherStreamDescriptor2))
  }

  private fun isMatch(
    stream: ConfiguredAirbyteStream,
    expected: StreamDescriptor,
  ): Boolean =
    StreamDescriptor()
      .withName(stream.stream.name)
      .withNamespace(stream.stream.namespace) == expected

  private fun <T> findStreamSyncMode(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    match: StreamDescriptor,
    syncModeFunction: Function<ConfiguredAirbyteStream, T>,
  ): T =
    configuredAirbyteCatalog.streams
      .stream()
      .filter { s: ConfiguredAirbyteStream -> isMatch(s, match) }
      .map(syncModeFunction)
      .findFirst()
      .get()

  private fun contains(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    match: StreamDescriptor,
  ): Boolean =
    configuredAirbyteCatalog.streams
      .stream()
      .filter { s: ConfiguredAirbyteStream -> isMatch(s, match) }
      .findFirst()
      .isPresent
}
