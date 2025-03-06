/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations;

import io.airbyte.commons.version.Version;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * Contains all protocol migrations.
 *
 * @param <T> protocol type
 */
public class MigrationContainer<T extends Migration> {

  private final List<T> migrationsToRegister;
  private final SortedMap<String, T> migrations = new TreeMap<>();

  // mostRecentMajorVersion defaults to v0 as no migration is required
  private String mostRecentMajorVersion = "0";

  public MigrationContainer(final List<T> migrations) {
    this.migrationsToRegister = migrations;
  }

  public void initialize() {
    migrationsToRegister.forEach(this::registerMigration);
  }

  public Version getMostRecentVersion() {
    return new Version(mostRecentMajorVersion, "0", "0");
  }

  /**
   * Downgrade a message from the most recent version to the target version by chaining all the
   * required migrations.
   */
  public <V0, V1> V0 downgrade(final V1 message,
                               final Version target,
                               final BiFunction<T, Object, Object> applyDowngrade) {
    if (target.getMajorVersion().equals(mostRecentMajorVersion)) {
      return (V0) message;
    }

    Object result = message;
    Object[] selectedMigrations = selectMigrations(target).toArray();
    for (int i = selectedMigrations.length; i > 0; --i) {
      result = applyDowngrade.apply((T) selectedMigrations[i - 1], result);
    }
    return (V0) result;
  }

  /**
   * Upgrade a message from the source version to the most recent version by chaining all the required
   * migrations.
   */
  public <V0, V1> V1 upgrade(final V0 message,
                             final Version source,
                             final BiFunction<T, Object, Object> applyUpgrade) {
    if (source.getMajorVersion().equals(mostRecentMajorVersion)) {
      return (V1) message;
    }

    Object result = message;
    for (var migration : selectMigrations(source)) {
      result = applyUpgrade.apply(migration, result);
    }
    return (V1) result;
  }

  /**
   * Get migrations needed to migrate to this version.
   *
   * @param version to migrate
   * @return needed migrations
   */
  public Collection<T> selectMigrations(final Version version) {
    final Collection<T> results = migrations.tailMap(version.getMajorVersion()).values();
    if (results.isEmpty()) {
      throw new RuntimeException("Unsupported migration version " + version.serialize());
    }
    return results;
  }

  /**
   * Store migration in a sorted map key by the major of the lower version of the migration.
   *
   * The goal is to be able to retrieve the list of migrations to apply to get to/from a given
   * version. We are only keying on the lower version because the right side (most recent version of
   * the migration range) is always current version.
   */
  private void registerMigration(final T migration) {
    final String key = migration.getPreviousVersion().getMajorVersion();
    if (!migrations.containsKey(key)) {
      migrations.put(key, migration);
      if (migration.getCurrentVersion().getMajorVersion().compareTo(mostRecentMajorVersion) > 0) {
        mostRecentMajorVersion = migration.getCurrentVersion().getMajorVersion();
      }
    } else {
      throw new RuntimeException("Trying to register a duplicated migration " + migration.getClass().getName());
    }
  }

  public Set<String> getMigrationKeys() {
    return migrations.keySet();
  }

}
