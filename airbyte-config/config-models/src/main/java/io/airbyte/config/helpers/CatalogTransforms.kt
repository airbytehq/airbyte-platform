/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.CatalogHelpers.Companion.extractDescriptor

/**
 * Utility class for transforming catalogs.
 */
object CatalogTransforms {
  /**
   * Updates a catalog for reset.
   */
  @JvmStatic
  fun updateCatalogForReset(
    streamsToReset: List<StreamDescriptor>,
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
  ) {
    val list: MutableList<ConfiguredAirbyteStream> = configuredAirbyteCatalog.streams.toMutableList()
    val iterator: MutableIterator<ConfiguredAirbyteStream> = list.iterator()
    while (iterator.hasNext()) {
      val configuredAirbyteStream = iterator.next()
      val streamDescriptor = extractDescriptor(configuredAirbyteStream)
      if (streamsToReset.contains(streamDescriptor)) {
        configuredAirbyteStream.syncMode = SyncMode.FULL_REFRESH
        configuredAirbyteStream.destinationSyncMode = DestinationSyncMode.OVERWRITE
      } else {
        iterator.remove()
      }
    }
    configuredAirbyteCatalog.streams = list
  }
}
