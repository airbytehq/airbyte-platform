/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.Optional;

/**
 * V1 Migration.
 */
// Disable V1 Migration, uncomment to re-enable
// @Singleton
public class AirbyteMessageMigrationV1 implements AirbyteMessageMigration<io.airbyte.protocol.models.v0.AirbyteMessage, AirbyteMessage> {

  @Override
  public io.airbyte.protocol.models.v0.AirbyteMessage downgrade(final AirbyteMessage oldMessage,
                                                                final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    return Jsons.object(
        Jsons.jsonNode(oldMessage),
        io.airbyte.protocol.models.v0.AirbyteMessage.class);
  }

  @Override
  public AirbyteMessage upgrade(final io.airbyte.protocol.models.v0.AirbyteMessage oldMessage,
                                final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    return Jsons.object(
        Jsons.jsonNode(oldMessage),
        AirbyteMessage.class);
  }

  @Override
  public Version getPreviousVersion() {
    return AirbyteProtocolVersion.V0;
  }

  @Override
  public Version getCurrentVersion() {
    return AirbyteProtocolVersion.V1;
  }

}
