/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.protocol.migrations.ConfiguredAirbyteCatalogMigration
import io.airbyte.commons.protocol.migrations.MigrationContainer
import io.airbyte.commons.version.Version
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton

/**
 * Migrates configured catalogs.
 */
@Singleton
class ConfiguredAirbyteCatalogMigrator(
  migrations: List<ConfiguredAirbyteCatalogMigration<*, *>>,
) {
  private val migrationContainer =
    MigrationContainer(migrations)

  @PostConstruct
  fun initialize() {
    migrationContainer.initialize()
  }

  /**
   * Downgrade a message from the most recent version to the target version by chaining all the
   * required migrations.
   */
  @Suppress("UNCHECKED_CAST")
  fun <PreviousVersion, CurrentVersion> downgrade(
    message: CurrentVersion,
    target: Version,
  ): PreviousVersion =
    migrationContainer.downgrade(
      message,
      target,
    ) { migration: ConfiguredAirbyteCatalogMigration<*, *>, message: Any ->
      applyDowngrade(
        migration as ConfiguredAirbyteCatalogMigration<Any, Any?>,
        message,
      )
    }

  /**
   * Upgrade a message from the source version to the most recent version by chaining all the required
   * migrations.
   */
  @Suppress("UNCHECKED_CAST")
  fun <PreviousVersion, CurrentVersion> upgrade(
    message: PreviousVersion,
    source: Version,
  ): CurrentVersion =
    migrationContainer.upgrade(
      message,
      source,
    ) { migration: ConfiguredAirbyteCatalogMigration<*, *>, message: Any ->
      applyUpgrade(
        migration as ConfiguredAirbyteCatalogMigration<Any, Any>,
        message,
      )
    }

  val mostRecentVersion: Version
    get() = migrationContainer.mostRecentVersion

  @get:VisibleForTesting
  val migrationKeys: Set<String>
    // Used for inspection of the injection
    get() = migrationContainer.migrationKeys

  companion object {
    // Helper function to work around type casting
    @Suppress("UNCHECKED_CAST")
    private fun <PreviousVersion, CurrentVersion> applyDowngrade(
      migration: ConfiguredAirbyteCatalogMigration<PreviousVersion, CurrentVersion>,
      message: Any,
    ): PreviousVersion = migration.downgrade(message as CurrentVersion)

    // Helper function to work around type casting
    @Suppress("UNCHECKED_CAST")
    private fun <PreviousVersion, CurrentVersion> applyUpgrade(
      migration: ConfiguredAirbyteCatalogMigration<PreviousVersion, CurrentVersion>,
      message: Any,
    ): CurrentVersion = migration.upgrade(message as PreviousVersion)
  }
}
