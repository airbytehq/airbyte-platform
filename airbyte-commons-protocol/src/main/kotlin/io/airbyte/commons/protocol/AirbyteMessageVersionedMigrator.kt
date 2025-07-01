/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.util.Optional

/**
 * Wraps message migration from a fixed version to the most recent version.
 *
 * M is the original message type.
 */
class AirbyteMessageVersionedMigrator<M>(
  private val migrator: AirbyteMessageMigrator,
  val version: Version,
) {
  fun downgrade(
    message: AirbyteMessage,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): M = migrator.downgrade(message, version, configuredAirbyteCatalog)

  fun upgrade(
    message: M,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): AirbyteMessage = migrator.upgrade(message, version, configuredAirbyteCatalog)
}
