package io.airbyte.commons.protocol

import io.airbyte.commons.protocol.DefaultProtocolSerializerTest.Companion.verifyDestinationSyncModesOverrides
import io.airbyte.commons.version.AirbyteProtocolVersion
import org.junit.jupiter.api.Test

class VersionedProtocolSerializerTest {
  @Test
  fun `verify we do not write certain destination sync modes serialization with refresh support`() {
    val serializer = VersionedProtocolSerializer(ConfiguredAirbyteCatalogMigrator(listOf()), AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
    verifyDestinationSyncModesOverrides(serializer, true)
  }

  @Test
  fun `verify we remain backward compatible for destination sync modes when refreshes are not supported`() {
    val serializer = VersionedProtocolSerializer(ConfiguredAirbyteCatalogMigrator(listOf()), AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
    verifyDestinationSyncModesOverrides(serializer, false)
  }
}
