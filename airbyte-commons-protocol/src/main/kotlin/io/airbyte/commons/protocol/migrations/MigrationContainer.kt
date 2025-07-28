/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations

import io.airbyte.commons.version.Version
import java.util.SortedMap
import java.util.TreeMap
import java.util.function.Consumer

/**
 * Contains all protocol migrations.
 *
 * @param <T> protocol type
</T> */
class MigrationContainer<T : Migration?>(
  private val migrationsToRegister: List<T>,
) {
  private val migrations: SortedMap<String, T> = TreeMap()

  // mostRecentMajorVersion defaults to v0 as no migration is required
  private var mostRecentMajorVersion = "0"

  fun initialize() {
    migrationsToRegister.forEach(Consumer { migration: T -> this.registerMigration(migration) })
  }

  val mostRecentVersion: Version
    get() = Version(mostRecentMajorVersion, "0", "0")

  /**
   * Downgrade a message from the most recent version to the target version by chaining all the
   * required migrations.
   */
  fun <V0, V1> downgrade(
    message: V1,
    target: Version,
    applyDowngrade: (T, Any) -> Any,
  ): V0 {
    if (target.getMajorVersion() == mostRecentMajorVersion) {
      return message as V0
    }

    var result: Any = message as Any
    val selectedMigrations: Collection<T> = selectMigrations(target)
    for (migration in selectedMigrations.reversed()) {
      result = applyDowngrade(migration, result)
    }
    return result as V0
  }

  /**
   * Upgrade a message from the source version to the most recent version by chaining all the required
   * migrations.
   */
  fun <V0, V1> upgrade(
    message: V0,
    source: Version,
    applyUpgrade: (T, Any) -> Any,
  ): V1 {
    if (source.getMajorVersion() == mostRecentMajorVersion) {
      return message as V1
    }

    var result: Any = message as Any
    for (migration in selectMigrations(source)) {
      result = applyUpgrade(migration, result)
    }
    return result as V1
  }

  /**
   * Get migrations needed to migrate to this version.
   *
   * @param version to migrate
   * @return needed migrations
   */
  fun selectMigrations(version: Version): Collection<T> {
    val results: Collection<T> = migrations.tailMap(version.getMajorVersion()).values
    if (results.isEmpty()) {
      throw RuntimeException("Unsupported migration version " + version.serialize())
    }
    return results
  }

  /**
   * Store migration in a sorted map key by the major of the lower version of the migration.
   *
   * The goal is to be able to retrieve the list of migrations to apply to get to/from a given
   * version. We are only keying on the lower version because the right side (most recent version of
   * the migration range) is always current version.
   */
  private fun registerMigration(migration: T) {
    val key = migration!!.getPreviousVersion().getMajorVersion()
    if (!migrations.containsKey(key)) {
      migrations[key] = migration
      if (migration.getCurrentVersion().getMajorVersion()!!.compareTo(mostRecentMajorVersion) > 0) {
        mostRecentMajorVersion = migration.getCurrentVersion().getMajorVersion()!!
      }
    } else {
      throw RuntimeException("Trying to register a duplicated migration " + migration.javaClass.name)
    }
  }

  val migrationKeys: Set<String>
    get() = migrations.keys
}
