/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations;

/**
 * ConfiguredCatalog Migration.
 *
 * @param <V0> previous version
 * @param <V1> current version
 */
public interface ConfiguredAirbyteCatalogMigration<V0, V1> extends Migration {

  /**
   * Downgrades a ConfiguredAirbyteCatalog from the new version to the old version.
   *
   * @param message the ConfiguredAirbyteCatalog to downgrade
   * @return the downgraded ConfiguredAirbyteCatalog
   */
  V0 downgrade(final V1 message);

  /**
   * Upgrades a ConfiguredAirbyteCatalog from the old version to the new version.
   *
   * @param message the ConfiguredAirbyteCatalog to upgrade
   * @return the upgraded ConfiguredAirbyteCatalog
   */
  V1 upgrade(final V0 message);

}
