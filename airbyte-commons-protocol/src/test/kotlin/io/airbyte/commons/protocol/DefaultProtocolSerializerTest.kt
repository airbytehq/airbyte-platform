package io.airbyte.commons.protocol

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import io.airbyte.protocol.models.AirbyteStream as ProtocolAirbyteStream
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteStream as ProtocolConfiguredAirbyteStream
import io.airbyte.protocol.models.DestinationSyncMode as ProtocolDestinationSyncMode

class DefaultProtocolSerializerTest {
  @Test
  fun `verify we do not write certain destination sync modes serialization with refresh support`() {
    val serializer = DefaultProtocolSerializer()
    verifyDestinationSyncModesOverrides(serializer, true)
  }

  @Test
  fun `verify we remain backward compatible for destination sync modes when refreshes are not supported`() {
    val serializer = DefaultProtocolSerializer()
    verifyDestinationSyncModesOverrides(serializer, false)
  }

  companion object {
    fun verifyDestinationSyncModesOverrides(
      serializer: ProtocolSerializer,
      supportRefreshes: Boolean,
    ) {
      val configuredCatalog =
        ConfiguredAirbyteCatalog()
          .withStreams(
            listOf(
              ConfiguredAirbyteStream()
                .withStream(AirbyteStream().withName("append"))
                .withDestinationSyncMode(DestinationSyncMode.APPEND),
              ConfiguredAirbyteStream()
                .withStream(AirbyteStream().withName("overwrite"))
                .withDestinationSyncMode(DestinationSyncMode.OVERWRITE),
              ConfiguredAirbyteStream()
                .withStream(AirbyteStream().withName("append_dedup"))
                .withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP),
              ConfiguredAirbyteStream()
                .withStream(AirbyteStream().withName("overwrite_dedup"))
                .withDestinationSyncMode(DestinationSyncMode.OVERWRITE_DEDUP),
            ),
          )
      val frozenConfiguredCatalog = Jsons.clone(configuredCatalog)

      val expectedConfiguredCatalog =
        ProtocolConfiguredAirbyteCatalog()
          .withStreams(
            listOf(
              ProtocolConfiguredAirbyteStream()
                .withStream(ProtocolAirbyteStream().withName("append"))
                .withDestinationSyncMode(ProtocolDestinationSyncMode.APPEND),
              ProtocolConfiguredAirbyteStream()
                .withStream(ProtocolAirbyteStream().withName("overwrite"))
                .withDestinationSyncMode(if (supportRefreshes) ProtocolDestinationSyncMode.APPEND else ProtocolDestinationSyncMode.OVERWRITE),
              ProtocolConfiguredAirbyteStream()
                .withStream(ProtocolAirbyteStream().withName("append_dedup"))
                .withDestinationSyncMode(ProtocolDestinationSyncMode.APPEND_DEDUP),
              ProtocolConfiguredAirbyteStream()
                .withStream(ProtocolAirbyteStream().withName("overwrite_dedup"))
                .withDestinationSyncMode(if (supportRefreshes) ProtocolDestinationSyncMode.APPEND_DEDUP else ProtocolDestinationSyncMode.OVERWRITE),
            ),
          )

      val serializedCatalog = serializer.serialize(configuredCatalog, supportRefreshes)
      val actualCatalog = Jsons.deserialize(serializedCatalog, io.airbyte.protocol.models.ConfiguredAirbyteCatalog::class.java)

      // Verify we serialized what's expected
      assertEquals(expectedConfiguredCatalog, actualCatalog)

      // Verify we didn't mutate the input
      assertEquals(frozenConfiguredCatalog, configuredCatalog)
    }
  }
}
