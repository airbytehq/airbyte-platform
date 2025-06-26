/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration
import io.airbyte.commons.protocol.migrations.MigrationContainer
import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import java.util.Optional

/**
 * AirbyteProtocol Message Migrator.
 *
 * This class is intended to apply the transformations required to go from one version of the
 * AirbyteProtocol to another.
 */
@Singleton
class AirbyteMessageMigrator(
  migrations: List<AirbyteMessageMigration<*, *>>,
) {
  private val migrationContainer = MigrationContainer(migrations)

  @PostConstruct
  fun initialize() {
    migrationContainer.initialize()
  }

  /**
   * Downgrade a message from the most recent version to the target version by chaining all the
   * required migrations.
   *
   * @param message message to upgrade
   * @param target target version ?
   * @param configuredAirbyteCatalog catalog
   * @param <PreviousVersion> version of message
   * @param <CurrentVersion> version to go to
   * @return downgraded catalog
   </CurrentVersion></PreviousVersion> */
  @Suppress("UNCHECKED_CAST")
  fun <PreviousVersion, CurrentVersion> downgrade(
    message: CurrentVersion,
    target: Version,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): PreviousVersion =
    migrationContainer.downgrade(
      message,
      target,
    ) { migration: AirbyteMessageMigration<*, *>, msg: Any ->
      applyDowngrade(
        migration as AirbyteMessageMigration<Any, Any?>,
        msg,
        configuredAirbyteCatalog,
      )
    }

  /**
   * Upgrade a message from the source version to the most recent version by chaining all the required
   * migrations.
   *
   * @param message message to upgrade
   * @param source source's version ?
   * @param configuredAirbyteCatalog catalog
   * @param <PreviousVersion> version of message
   * @param <CurrentVersion> version to go to
   * @return upgraded catalog
   </CurrentVersion></PreviousVersion> */
  @Suppress("UNCHECKED_CAST")
  fun <PreviousVersion, CurrentVersion> upgrade(
    message: PreviousVersion,
    source: Version,
    configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
  ): CurrentVersion =
    migrationContainer.upgrade(
      message,
      source,
    ) { migration: AirbyteMessageMigration<*, *>, msg: Any ->
      applyUpgrade(
        migration as AirbyteMessageMigration<Any, Any>,
        msg,
        configuredAirbyteCatalog,
      )
    }

  val mostRecentVersion: Version
    /**
     * Get most recent protocol version.
     *
     * @return protocol version
     */
    get() = migrationContainer.mostRecentVersion

  @get:VisibleForTesting
  val migrationKeys: Set<String>
    // Used for inspection of the injection
    get() = migrationContainer.migrationKeys

  companion object {
    // Helper function to work around type casting
    @Suppress("UNCHECKED_CAST")
    private fun <PreviousVersion, CurrentVersion> applyDowngrade(
      migration: AirbyteMessageMigration<PreviousVersion, CurrentVersion>,
      message: Any,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): PreviousVersion = migration.downgrade(message as CurrentVersion, configuredAirbyteCatalog)

    // Helper function to work around type casting
    @Suppress("UNCHECKED_CAST")
    private fun <PreviousVersion, CurrentVersion> applyUpgrade(
      migration: AirbyteMessageMigration<PreviousVersion, CurrentVersion>,
      message: Any,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): CurrentVersion = migration.upgrade(message as PreviousVersion, configuredAirbyteCatalog)
  }
}
