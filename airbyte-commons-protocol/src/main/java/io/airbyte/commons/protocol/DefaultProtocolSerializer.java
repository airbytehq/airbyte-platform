/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;

/**
 * Default JSON serialization for the Airbyte Protocol.
 */
public class DefaultProtocolSerializer implements ProtocolSerializer {

  @Override
  public String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final boolean supportsRefreshes) {
    return Jsons.serialize(replaceDestinationSyncModes(configuredAirbyteCatalog, supportsRefreshes));
  }

  static ConfiguredAirbyteCatalog replaceDestinationSyncModes(final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                              final boolean supportsRefreshes) {
    // Copy to avoid mutating input
    final ConfiguredAirbyteCatalog clonedCatalog = Jsons.clone(configuredAirbyteCatalog);

    // Ensure we convert destination sync modes to the expected ones
    for (final ConfiguredAirbyteStream stream : clonedCatalog.getStreams()) {
      if (supportsRefreshes) {
        if (DestinationSyncMode.OVERWRITE.equals(stream.getDestinationSyncMode())) {
          stream.setDestinationSyncMode(DestinationSyncMode.APPEND);
        } else if (DestinationSyncMode.OVERWRITE_DEDUP.equals(stream.getDestinationSyncMode())) {
          stream.setDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP);
        }
      } else {
        if (DestinationSyncMode.OVERWRITE_DEDUP.equals(stream.getDestinationSyncMode())) {
          stream.setDestinationSyncMode(DestinationSyncMode.OVERWRITE);
        }
      }
    }
    return clonedCatalog;
  }

}
