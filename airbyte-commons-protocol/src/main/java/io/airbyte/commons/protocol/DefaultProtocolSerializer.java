/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.helpers.ProtocolConverters;

/**
 * Default JSON serialization for the Airbyte Protocol.
 */
public class DefaultProtocolSerializer implements ProtocolSerializer {

  @Override
  public String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final boolean supportsRefreshes) {
    // Copy to avoid mutating input
    final ConfiguredAirbyteCatalog clonedCatalog = Jsons.clone(configuredAirbyteCatalog);
    replaceDestinationSyncModes(clonedCatalog, supportsRefreshes);

    return Jsons.serialize(toProtocol(clonedCatalog));
  }

  private void replaceDestinationSyncModes(final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final boolean supportsRefreshes) {
    // Ensure we convert destination sync modes to the expected ones
    for (final ConfiguredAirbyteStream stream : configuredAirbyteCatalog.getStreams()) {
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
  }

  /**
   * Protocol conversion helper.
   * <p>
   * This is private as the to protocol serialization should be handled through the serializer rather
   * than a plain to class conversion because we may adapt data based on the protocol version.
   */
  private io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog toProtocol(final ConfiguredAirbyteCatalog catalog) {
    return new io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog()
        .withStreams(catalog.getStreams().stream().map(this::toProtocol).toList());
  }

  /**
   * Protocol conversion helper.
   * <p>
   * This is private as the to protocol serialization should be handled through the serializer rather
   * than a plain to class conversion because we may adapt data based on the protocol version.
   */
  private io.airbyte.protocol.models.v0.ConfiguredAirbyteStream toProtocol(final ConfiguredAirbyteStream stream) {
    return new io.airbyte.protocol.models.v0.ConfiguredAirbyteStream()
        .withStream(ProtocolConverters.toProtocol(stream.getStream()))
        .withSyncMode(Enums.convertTo(stream.getSyncMode(), io.airbyte.protocol.models.v0.SyncMode.class))
        .withDestinationSyncMode(Enums.convertTo(stream.getDestinationSyncMode(), io.airbyte.protocol.models.v0.DestinationSyncMode.class))
        .withCursorField(stream.getCursorField())
        .withPrimaryKey(stream.getPrimaryKey())
        .withGenerationId(stream.getGenerationId())
        .withMinimumGenerationId(stream.getMinimumGenerationId())
        .withIncludeFiles(stream.getIncludeFiles())
        .withDestinationObjectName(stream.getDestinationObjectName())
        .withSyncId(stream.getSyncId());
  }

}
