/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.migrations.ConfiguredAirbyteCatalogMigration;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

/**
 * ConfiguredCatalog V1 Migration.
 */
// Disable V1 Migration, uncomment to re-enable
// @Singleton
public class ConfiguredAirbyteCatalogMigrationV1
    implements ConfiguredAirbyteCatalogMigration<io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog, ConfiguredAirbyteCatalog> {

  @Override
  public io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog downgrade(final ConfiguredAirbyteCatalog oldMessage) {
    return Jsons.object(
        Jsons.jsonNode(oldMessage),
        io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog.class);
  }

  @Override
  public ConfiguredAirbyteCatalog upgrade(final io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog oldMessage) {
    return Jsons.object(
        Jsons.jsonNode(oldMessage),
        ConfiguredAirbyteCatalog.class);
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
