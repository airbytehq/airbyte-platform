/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class CatalogTransformsTest {

  @Test
  void testResetCatalogSyncModeReplacement() throws IOException {
    final String catalog = MoreResources.readResource("catalogs/simple_catalog.json");
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = Jsons.object(Jsons.deserialize(catalog),
        ConfiguredAirbyteCatalog.class);
    final StreamDescriptor streamDescriptor = new StreamDescriptor().withName("pokemon");
    final List<StreamDescriptor> streamsToReset = List.of(streamDescriptor);

    CatalogTransforms.updateCatalogForReset(streamsToReset, configuredAirbyteCatalog);

    assertEquals(DestinationSyncMode.OVERWRITE, findStreamSyncMode(configuredAirbyteCatalog,
        streamDescriptor, ConfiguredAirbyteStream::getDestinationSyncMode));
    assertEquals(SyncMode.FULL_REFRESH, findStreamSyncMode(configuredAirbyteCatalog,
        streamDescriptor, ConfiguredAirbyteStream::getSyncMode));
  }

  @Test
  void testResetCatalogSyncModeReplacementMultipleStreams() throws IOException {
    final String catalog = MoreResources.readResource("catalogs/multiple_stream_catalog.json");
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = Jsons.object(Jsons.deserialize(catalog),
        ConfiguredAirbyteCatalog.class);
    final StreamDescriptor streamDescriptor = new StreamDescriptor().withName("pokemon");
    final StreamDescriptor otherStreamDescriptor = new StreamDescriptor().withName("other");
    final StreamDescriptor otherStreamDescriptor2 = new StreamDescriptor().withName("other2").withNamespace("namespace");
    final List<StreamDescriptor> streamsToReset = List.of(streamDescriptor);

    CatalogTransforms.updateCatalogForReset(streamsToReset, configuredAirbyteCatalog);

    assertEquals(DestinationSyncMode.OVERWRITE, findStreamSyncMode(configuredAirbyteCatalog,
        streamDescriptor, ConfiguredAirbyteStream::getDestinationSyncMode));
    assertEquals(SyncMode.FULL_REFRESH, findStreamSyncMode(configuredAirbyteCatalog,
        streamDescriptor, ConfiguredAirbyteStream::getSyncMode));
    assertFalse(contains(configuredAirbyteCatalog, otherStreamDescriptor));
    assertFalse(contains(configuredAirbyteCatalog, otherStreamDescriptor2));
  }

  private boolean isMatch(final ConfiguredAirbyteStream stream, final StreamDescriptor expected) {
    return new StreamDescriptor().withName(stream.getStream().getName())
        .withNamespace(stream.getStream().getNamespace()).equals(expected);
  }

  private <T> T findStreamSyncMode(final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                   final StreamDescriptor match,
                                   final Function<ConfiguredAirbyteStream, T> syncModeFunction) {
    return configuredAirbyteCatalog.getStreams()
        .stream()
        .filter(s -> isMatch(s, match))
        .map(syncModeFunction)
        .findFirst().get();
  }

  private boolean contains(final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                           final StreamDescriptor match) {
    return configuredAirbyteCatalog.getStreams()
        .stream()
        .filter(s -> isMatch(s, match))
        .findFirst()
        .isPresent();
  }

}
