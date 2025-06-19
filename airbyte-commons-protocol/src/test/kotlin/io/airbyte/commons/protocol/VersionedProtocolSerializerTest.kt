/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.protocol.DefaultProtocolSerializerTest.Companion.verifyDestinationSyncModesOverrides
import io.airbyte.commons.version.AirbyteProtocolVersion
import org.junit.jupiter.api.Test

class VersionedProtocolSerializerTest {
  @Test
  fun `verify we do not write certain destination sync modes serialization with refresh support`() {
    val serializer = VersionedProtocolSerializer(ConfiguredAirbyteCatalogMigrator(listOf()), AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
    verifyDestinationSyncModesOverrides(serializer, true, SerializationTarget.DESTINATION)
  }

  @Test
  fun `verify we remain backward compatible for destination sync modes when refreshes are not supported`() {
    val serializer = VersionedProtocolSerializer(ConfiguredAirbyteCatalogMigrator(listOf()), AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
    verifyDestinationSyncModesOverrides(serializer, false, SerializationTarget.DESTINATION)
  }

  @Test
  fun `verify we do not write data activation destination sync modes to the sources`() {
    val serializer = VersionedProtocolSerializer(ConfiguredAirbyteCatalogMigrator(listOf()), AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
    verifyDestinationSyncModesOverrides(serializer, true, SerializationTarget.SOURCE)
  }
}
