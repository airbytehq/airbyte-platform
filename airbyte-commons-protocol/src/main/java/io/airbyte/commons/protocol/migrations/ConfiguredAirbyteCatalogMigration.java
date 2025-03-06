/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations;

/**
 * Migrate between different version sof airbyte catalog.
 *
 * @param <PreviousVersion> previous version
 * @param <CurrentVersion> current version
 */
@SuppressWarnings("InterfaceTypeParameterName")
public interface ConfiguredAirbyteCatalogMigration<PreviousVersion, CurrentVersion> extends Migration {

  /**
   * Downgrades a ConfiguredAirbyteCatalog from the new version to the old version.
   *
   * @param message the ConfiguredAirbyteCatalog to downgrade
   * @return the downgraded ConfiguredAirbyteCatalog
   */
  PreviousVersion downgrade(final CurrentVersion message);

  /**
   * Upgrades a ConfiguredAirbyteCatalog from the old version to the new version.
   *
   * @param message the ConfiguredAirbyteCatalog to upgrade
   * @return the upgraded ConfiguredAirbyteCatalog
   */
  CurrentVersion upgrade(final PreviousVersion message);

}
