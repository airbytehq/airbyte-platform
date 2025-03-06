/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import java.util.Optional;

/**
 * Wraps message migration from a fixed version to the most recent version.
 *
 * M is the original message type.
 */
public class AirbyteMessageVersionedMigrator<M> {

  private final AirbyteMessageMigrator migrator;
  private final Version version;

  public AirbyteMessageVersionedMigrator(final AirbyteMessageMigrator migrator, final Version version) {
    this.migrator = migrator;
    this.version = version;
  }

  public M downgrade(final AirbyteMessage message, final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    return migrator.downgrade(message, version, configuredAirbyteCatalog);
  }

  public AirbyteMessage upgrade(final M message, final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    return migrator.upgrade(message, version, configuredAirbyteCatalog);
  }

  public Version getVersion() {
    return version;
  }

}
