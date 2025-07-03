/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog

/**
 * Serialize a ConfiguredAirbyteCatalog to the specified version.
 *
 *
 * This Serializer expects a ConfiguredAirbyteCatalog from the Current version of the platform,
 * converts it to the target protocol version before serializing it.
 */
class VersionedProtocolSerializer(
  private val configuredAirbyteCatalogMigrator: ConfiguredAirbyteCatalogMigrator,
  private val protocolVersion: Version,
) : ProtocolSerializer {
  override fun serialize(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    supportsRefreshes: Boolean,
    target: SerializationTarget,
  ): String {
    // TODO: rework the migration part to support different protocol version. This currently works
    // because we only have one major.
    return DefaultProtocolSerializer().serialize(configuredAirbyteCatalog, supportsRefreshes, target)
  }
}
