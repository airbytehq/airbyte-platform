/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ConfiguredAirbyteCatalog;

/**
 * Serialize a ConfiguredAirbyteCatalog to the specified version.
 * <p>
 * This Serializer expects a ConfiguredAirbyteCatalog from the Current version of the platform,
 * converts it to the target protocol version before serializing it.
 */
public class VersionedProtocolSerializer implements ProtocolSerializer {

  @SuppressWarnings("PMD.UnusedPrivateField")
  private final ConfiguredAirbyteCatalogMigrator configuredAirbyteCatalogMigrator;

  @SuppressWarnings("PMD.UnusedPrivateField")
  private final Version protocolVersion;

  public VersionedProtocolSerializer(final ConfiguredAirbyteCatalogMigrator configuredAirbyteCatalogMigrator, final Version protocolVersion) {
    this.configuredAirbyteCatalogMigrator = configuredAirbyteCatalogMigrator;
    this.protocolVersion = protocolVersion;
  }

  @Override
  public String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final boolean supportsRefreshes) {
    // TODO: rework the migration part to support different protocol version. This currently works
    // because we only have one major.
    return new DefaultProtocolSerializer().serialize(configuredAirbyteCatalog, supportsRefreshes);
  }

}
