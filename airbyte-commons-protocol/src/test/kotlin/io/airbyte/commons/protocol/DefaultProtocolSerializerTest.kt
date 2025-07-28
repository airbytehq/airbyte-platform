/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.SyncMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import io.airbyte.protocol.models.v0.AirbyteStream as ProtocolAirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream as ProtocolConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode as ProtocolDestinationSyncMode

class DefaultProtocolSerializerTest {
  @Test
  fun `verify we do not write certain destination sync modes serialization with refresh support`() {
    val serializer = DefaultProtocolSerializer()
    verifyDestinationSyncModesOverrides(serializer, true, SerializationTarget.DESTINATION)
  }

  @Test
  fun `verify we remain backward compatible for destination sync modes when refreshes are not supported`() {
    val serializer = DefaultProtocolSerializer()
    verifyDestinationSyncModesOverrides(serializer, false, SerializationTarget.DESTINATION)
  }

  @Test
  fun `verify we do not write data activation destination sync modes to the sources`() {
    val serializer = DefaultProtocolSerializer()
    verifyDestinationSyncModesOverrides(serializer, true, SerializationTarget.SOURCE)
  }

  companion object {
    fun verifyDestinationSyncModesOverrides(
      serializer: ProtocolSerializer,
      supportRefreshes: Boolean,
      target: SerializationTarget,
    ) {
      val appendStreamName = "append"
      val overwriteStreamName = "overwrite"
      val appendDedupStreamName = "append_dedup"
      val overwriteDedupStreamName = "overwrite_dedup"
      val updateStreamName = "update"
      val softDeleteStreamName = "soft_delete"

      val configuredCatalog =
        ConfiguredAirbyteCatalog()
          .withStreams(
            listOf(
              ConfiguredAirbyteStream(getAirbyteStream(appendStreamName), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
              ConfiguredAirbyteStream(getAirbyteStream(overwriteStreamName), SyncMode.FULL_REFRESH, DestinationSyncMode.OVERWRITE),
              ConfiguredAirbyteStream(getAirbyteStream(appendDedupStreamName), SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND_DEDUP),
              ConfiguredAirbyteStream(getAirbyteStream(overwriteDedupStreamName), SyncMode.FULL_REFRESH, DestinationSyncMode.OVERWRITE_DEDUP),
              ConfiguredAirbyteStream(getAirbyteStream(updateStreamName), SyncMode.FULL_REFRESH, DestinationSyncMode.UPDATE),
              ConfiguredAirbyteStream(getAirbyteStream(softDeleteStreamName), SyncMode.FULL_REFRESH, DestinationSyncMode.SOFT_DELETE),
            ),
          )
      val frozenConfiguredCatalog = Jsons.clone(configuredCatalog)

      val expectedConfiguredCatalog =
        ProtocolConfiguredAirbyteCatalog()
          .withStreams(
            listOf(
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      appendStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.INCREMENTAL)
                .withDestinationSyncMode(ProtocolDestinationSyncMode.APPEND)
                .withIncludeFiles(false),
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      overwriteStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)
                .withDestinationSyncMode(if (supportRefreshes) ProtocolDestinationSyncMode.APPEND else ProtocolDestinationSyncMode.OVERWRITE)
                .withIncludeFiles(false),
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      appendDedupStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)
                .withDestinationSyncMode(ProtocolDestinationSyncMode.APPEND_DEDUP)
                .withIncludeFiles(false),
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      overwriteDedupStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)
                .withDestinationSyncMode(if (supportRefreshes) ProtocolDestinationSyncMode.APPEND_DEDUP else ProtocolDestinationSyncMode.OVERWRITE)
                .withIncludeFiles(false),
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      updateStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)
                .withDestinationSyncMode(
                  if (target !=
                    SerializationTarget.SOURCE
                  ) {
                    ProtocolDestinationSyncMode.UPDATE
                  } else {
                    ProtocolDestinationSyncMode.APPEND
                  },
                ).withIncludeFiles(false),
              ProtocolConfiguredAirbyteStream()
                .withStream(
                  ProtocolAirbyteStream()
                    .withName(
                      softDeleteStreamName,
                    ).withJsonSchema(Jsons.emptyObject())
                    .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)),
                ).withSyncMode(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH)
                .withDestinationSyncMode(
                  if (target !=
                    SerializationTarget.SOURCE
                  ) {
                    ProtocolDestinationSyncMode.SOFT_DELETE
                  } else {
                    ProtocolDestinationSyncMode.APPEND
                  },
                ).withIncludeFiles(false),
            ),
          )

      val serializedCatalog = serializer.serialize(configuredCatalog, supportRefreshes, target)
      val actualCatalog = Jsons.deserialize(serializedCatalog, ProtocolConfiguredAirbyteCatalog::class.java)

      // Verify we serialized what's expected
      assertEquals(expectedConfiguredCatalog, actualCatalog)

      // Verify we didn't mutate the input
      assertEquals(frozenConfiguredCatalog, configuredCatalog)
    }

    fun getAirbyteStream(name: String) = AirbyteStream(name, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH))
  }
}
