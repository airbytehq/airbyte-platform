/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.protocol.models.v0.SyncMode
import io.micronaut.core.util.CollectionUtils

/**
 * Default JSON serialization for the Airbyte Protocol.
 */
class DefaultProtocolSerializer : ProtocolSerializer {
  override fun serialize(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    supportsRefreshes: Boolean,
    target: SerializationTarget,
  ): String {
    // Copy to avoid mutating input
    val clonedCatalog = Jsons.clone(configuredAirbyteCatalog)
    replaceDestinationSyncModes(clonedCatalog, supportsRefreshes, target)

    return Jsons.serialize(toProtocol(clonedCatalog))
  }

  private fun replaceDestinationSyncModes(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    supportsRefreshes: Boolean,
    target: SerializationTarget,
  ) {
    // Ensure we convert destination sync modes to the expected ones
    for (stream in configuredAirbyteCatalog.streams) {
      if (target == SerializationTarget.SOURCE) {
        // New destination sync modes were added to for data activation destinations.
        // However, because DestinationSyncModes are an enum and currently passed to sources even though the
        // value is irrelevant to the sources,
        // they end up failing to deserialize the configured catalog.
        // This hides the new sync modes from all sources, until we effectively split the configured catalog
        // into a source and destination version.
        if (legacyDestinationSyncModes.contains(stream.destinationSyncMode)) {
          stream.destinationSyncMode = getNonDataActivationDestinationSyncMode(stream.destinationSyncMode, supportsRefreshes)
        } else {
          stream.destinationSyncMode = DestinationSyncMode.APPEND
        }
      } else {
        stream.destinationSyncMode = getNonDataActivationDestinationSyncMode(stream.destinationSyncMode, supportsRefreshes)
      }
    }
  }

  private fun getNonDataActivationDestinationSyncMode(
    syncMode: DestinationSyncMode,
    supportsRefreshes: Boolean,
  ): DestinationSyncMode {
    if (supportsRefreshes) {
      if (DestinationSyncMode.OVERWRITE == syncMode) {
        return DestinationSyncMode.APPEND
      } else if (DestinationSyncMode.OVERWRITE_DEDUP == syncMode) {
        return DestinationSyncMode.APPEND_DEDUP
      }
    } else {
      if (DestinationSyncMode.OVERWRITE_DEDUP == syncMode) {
        return DestinationSyncMode.OVERWRITE
      }
    }
    return syncMode
  }

  /**
   * Protocol conversion helper.
   *
   *
   * This is private as the to protocol serialization should be handled through the serializer rather
   * than a plain to class conversion because we may adapt data based on the protocol version.
   */
  private fun toProtocol(catalog: ConfiguredAirbyteCatalog): io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog =
    io.airbyte.protocol.models.v0
      .ConfiguredAirbyteCatalog()
      .withStreams(
        catalog.streams
          .stream()
          .map { stream: ConfiguredAirbyteStream -> this.toProtocol(stream) }
          .toList(),
      )

  /**
   * Protocol conversion helper.
   *
   *
   * This is private as the to protocol serialization should be handled through the serializer rather
   * than a plain to class conversion because we may adapt data based on the protocol version.
   */
  private fun toProtocol(stream: ConfiguredAirbyteStream): io.airbyte.protocol.models.v0.ConfiguredAirbyteStream =
    io.airbyte.protocol.models.v0
      .ConfiguredAirbyteStream()
      .withStream(stream.stream.toProtocol())
      .withSyncMode(stream.syncMode.convertTo<SyncMode>())
      .withDestinationSyncMode(
        stream.destinationSyncMode.convertTo<io.airbyte.protocol.models.v0.DestinationSyncMode>(),
      ).withCursorField(stream.cursorField)
      .withPrimaryKey(stream.primaryKey)
      .withGenerationId(stream.generationId)
      .withMinimumGenerationId(stream.minimumGenerationId)
      .withIncludeFiles(stream.includeFiles)
      .withDestinationObjectName(stream.destinationObjectName)
      .withSyncId(stream.syncId)

  companion object {
    private val legacyDestinationSyncModes: Set<DestinationSyncMode> =
      CollectionUtils.setOf(
        DestinationSyncMode.APPEND,
        DestinationSyncMode.APPEND_DEDUP,
        DestinationSyncMode.OVERWRITE,
        DestinationSyncMode.OVERWRITE_DEDUP,
      )
  }
}
