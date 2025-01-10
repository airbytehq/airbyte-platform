/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;

/**
 * V1 Migration.
 */
// Disable V1 Migration, uncomment to re-enable
public class AirbyteMessageMigrationV1 implements AirbyteMessageMigration<io.airbyte.protocol.models.v0.AirbyteMessage, AirbyteMessage> {

  @Override
  public io.airbyte.protocol.models.v0.AirbyteMessage downgrade(AirbyteMessage message, Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    throw new NotImplementedException("Migration not implemented.");
  }

  @Override
  public AirbyteMessage upgrade(io.airbyte.protocol.models.v0.AirbyteMessage message, Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    throw new NotImplementedException("Migration not implemented.");
  }

  @Override
  public Version getPreviousVersion() {
    return null;
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

}
