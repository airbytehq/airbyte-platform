/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class for transforming catalogs.
 */
public class CatalogTransforms {

  /**
   * Updates a catalog for reset.
   */
  public static void updateCatalogForReset(
                                           final List<StreamDescriptor> streamsToReset,
                                           final ConfiguredAirbyteCatalog configuredAirbyteCatalog) {
    Iterator<ConfiguredAirbyteStream> iterator = configuredAirbyteCatalog.getStreams().iterator();
    while (iterator.hasNext()) {
      ConfiguredAirbyteStream configuredAirbyteStream = iterator.next();
      final StreamDescriptor streamDescriptor = CatalogHelpers.extractDescriptor(configuredAirbyteStream);
      if (streamsToReset.contains(streamDescriptor)) {
        // The Reset Source will emit no record messages for any streams, so setting the destination sync
        // mode to OVERWRITE will empty out this stream in the destination.
        // Note: streams in streamsToReset that are NOT in this configured catalog (i.e. deleted streams)
        // will still have their state reset by the Reset Source, but will not be modified in the
        // destination since they are not present in the catalog that is sent to the destination.
        configuredAirbyteStream.setSyncMode(SyncMode.FULL_REFRESH);
        configuredAirbyteStream.setDestinationSyncMode(DestinationSyncMode.OVERWRITE);
      } else {
        // Remove the streams that are not being reset.
        iterator.remove();
      }
    }

  }

}
