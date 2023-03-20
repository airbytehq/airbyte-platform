/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations;

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.Optional;

/**
 * AirbyteProtocol message migration interface.
 *
 * @param <V0> The Old AirbyteMessage type
 * @param <V1> The New AirbyteMessage type
 */
public interface AirbyteMessageMigration<V0, V1> extends Migration {

  /**
   * Downgrades a message to from the new version to the old version.
   *
   * @param message the message to downgrade
   * @param configuredAirbyteCatalog the ConfiguredAirbyteCatalog of the connection when applicable
   * @return the downgraded message
   */
  V0 downgrade(final V1 message, final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog);

  /**
   * Upgrades a message from the old version to the new
   * version./Users/charles/code/airbyte-platform/airbyte-commons-protocol/src/main/java/io/airbyte/commons/protocol/migrations/Migration.java
   *
   *
   * @param configuredAirbyteCatalog the ConfiguredAirbyteCatalog of the connection when applicable
   * @return the upgrade message
   */
  V1 upgrade(final V0 message, final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog);

}
