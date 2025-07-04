/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations

import io.airbyte.config.ConfiguredAirbyteCatalog
import java.util.Optional

/**
 * AirbyteProtocol message migration interface.
 *
 * @param <V0> The Old AirbyteMessage type
 * @param <V1> The New AirbyteMessage type
</V1></V0> */
interface AirbyteMessageMigration<V0, V1> : Migration {
  /**
   * Downgrades a message to from the new version to the old version.
   *
   * @param message the message to downgrade
   * @param configuredAirbyteCatalog the ConfiguredAirbyteCatalog of the connection when applicable
   * @return the downgraded message
   */
  fun downgrade(
    message: V1,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): V0

  /**
   * Upgrades a message from the old version to the new version.
   *
   * @param configuredAirbyteCatalog the ConfiguredAirbyteCatalog of the connection when applicable
   * @return the upgrade message
   */
  fun upgrade(
    message: V0,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): V1
}
