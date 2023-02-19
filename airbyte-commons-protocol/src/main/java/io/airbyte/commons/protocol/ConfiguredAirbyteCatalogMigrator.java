/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.protocol.migrations.ConfiguredAirbyteCatalogMigration;
import io.airbyte.commons.protocol.migrations.MigrationContainer;
import io.airbyte.commons.version.Version;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Set;

/**
 * Catalog migrator.
 */
@Singleton
public class ConfiguredAirbyteCatalogMigrator {

  private final MigrationContainer<ConfiguredAirbyteCatalogMigration<?, ?>> migrationContainer;

  public ConfiguredAirbyteCatalogMigrator(final List<ConfiguredAirbyteCatalogMigration<?, ?>> migrations) {
    migrationContainer = new MigrationContainer<>(migrations);
  }

  @PostConstruct
  public void initialize() {
    migrationContainer.initialize();
  }

  /**
   * Downgrade a message from the most recent version to the target version by chaining all the
   * required migrations.
   */
  public <V0, V1> V0 downgrade(final V1 message, final Version target) {
    return migrationContainer.downgrade(message, target, ConfiguredAirbyteCatalogMigrator::applyDowngrade);
  }

  /**
   * Upgrade a message from the source version to the most recent version by chaining all the required
   * migrations.
   */
  public <V0, V1> V1 upgrade(final V0 message, final Version source) {
    return migrationContainer.upgrade(message, source, ConfiguredAirbyteCatalogMigrator::applyUpgrade);
  }

  public Version getMostRecentVersion() {
    return migrationContainer.getMostRecentVersion();
  }

  // Helper function to work around type casting
  private static <V0, V1> V0 applyDowngrade(final ConfiguredAirbyteCatalogMigration<V0, V1> migration,
                                            final Object message) {
    return migration.downgrade((V1) message);
  }

  // Helper function to work around type casting
  private static <V0, V1> V1 applyUpgrade(final ConfiguredAirbyteCatalogMigration<V0, V1> migration,
                                          final Object message) {
    return migration.upgrade((V0) message);
  }

  // Used for inspection of the injection
  @VisibleForTesting
  Set<String> getMigrationKeys() {
    return migrationContainer.getMigrationKeys();
  }

}
