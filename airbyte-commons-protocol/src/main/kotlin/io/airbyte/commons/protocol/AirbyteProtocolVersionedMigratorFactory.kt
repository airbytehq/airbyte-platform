/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.version.Version
import jakarta.inject.Singleton

/**
 * Factory to build AirbyteMessageVersionedMigrator.
 */
@Singleton
class AirbyteProtocolVersionedMigratorFactory(
  private val airbyteMessageMigrator: AirbyteMessageMigrator,
  private val configuredAirbyteCatalogMigrator: ConfiguredAirbyteCatalogMigrator,
) {
  fun <T> getAirbyteMessageMigrator(version: Version): AirbyteMessageVersionedMigrator<T> =
    AirbyteMessageVersionedMigrator(airbyteMessageMigrator, version)

  fun getProtocolSerializer(version: Version): VersionedProtocolSerializer = VersionedProtocolSerializer(configuredAirbyteCatalogMigrator, version)

  val mostRecentVersion: Version
    get() = airbyteMessageMigrator.mostRecentVersion
}
