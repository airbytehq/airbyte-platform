/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import static io.micronaut.core.util.CollectionUtils.setOf;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.helpers.ProtocolConverters;
import java.util.Set;

/**
 * Default JSON serialization for the Airbyte Protocol.
 */
public class DefaultProtocolSerializer implements ProtocolSerializer {

  private static final Set<DestinationSyncMode> legacyDestinationSyncModes = setOf(
      DestinationSyncMode.APPEND,
      DestinationSyncMode.APPEND_DEDUP,
      DestinationSyncMode.OVERWRITE,
      DestinationSyncMode.OVERWRITE_DEDUP);

  @Override
  public String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                          final boolean supportsRefreshes,
                          final SerializationTarget target) {
    // Copy to avoid mutating input
    final ConfiguredAirbyteCatalog clonedCatalog = Jsons.clone(configuredAirbyteCatalog);
    replaceDestinationSyncModes(clonedCatalog, supportsRefreshes, target);

    return Jsons.serialize(toProtocol(clonedCatalog));
  }

  private void replaceDestinationSyncModes(final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                           final boolean supportsRefreshes,
                                           final SerializationTarget target) {
    // Ensure we convert destination sync modes to the expected ones
    for (final ConfiguredAirbyteStream stream : configuredAirbyteCatalog.getStreams()) {
      if (target == SerializationTarget.SOURCE) {
        // New destination sync modes were added to for data activation destinations.
        // However, because DestinationSyncModes are an enum and currently passed to sources even though the
        // value is irrelevant to the sources,
        // they end up failing to deserialize the configured catalog.
        // This hides the new sync modes from all sources, until we effectively split the configured catalog
        // into a source and destination version.
        if (legacyDestinationSyncModes.contains(stream.getDestinationSyncMode())) {
          stream.setDestinationSyncMode(getNonDataActivationDestinationSyncMode(stream.getDestinationSyncMode(), supportsRefreshes));
        } else {
          stream.setDestinationSyncMode(DestinationSyncMode.APPEND);
        }
      } else {
        stream.setDestinationSyncMode(getNonDataActivationDestinationSyncMode(stream.getDestinationSyncMode(), supportsRefreshes));
      }
    }
  }

  private DestinationSyncMode getNonDataActivationDestinationSyncMode(final DestinationSyncMode syncMode, Boolean supportsRefreshes) {
    if (supportsRefreshes) {
      if (DestinationSyncMode.OVERWRITE.equals(syncMode)) {
        return DestinationSyncMode.APPEND;
      } else if (DestinationSyncMode.OVERWRITE_DEDUP.equals(syncMode)) {
        return DestinationSyncMode.APPEND_DEDUP;
      }
    } else {
      if (DestinationSyncMode.OVERWRITE_DEDUP.equals(syncMode)) {
        return DestinationSyncMode.OVERWRITE;
      }
    }
    return syncMode;
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
