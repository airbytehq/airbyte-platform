/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import org.flywaydb.core.Flyway
import org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension
import javax.sql.DataSource

/**
 * Temporary factory class that provides convenience methods for creating a [Flyway]
 * instances. This class will be removed once the project has been converted to leverage an
 * application framework to manage the creation and injection of [Flyway] objects.
 */
object FlywayFactory {
  const val MIGRATION_TABLE_FORMAT: String = "airbyte_%s_migrations"

  // Constants for Flyway baseline. See here for details:
  // https://flywaydb.org/documentation/command/baseline
  const val BASELINE_VERSION: String = "0.29.0.001"
  const val BASELINE_DESCRIPTION: String = "Baseline from file-based migration v1"
  const val BASELINE_ON_MIGRATION: Boolean = true

  /**
   * Constructs a configured [Flyway] instance using the provided configuration.
   *
   * @param dataSource The [DataSource] used to connect to the database.
   * @param installedBy The name of the module performing the migration.
   * @param dbIdentifier The name of the database to be migrated. This is used to name the table to
   * hold the migration history for the database.
   * @param migrationFileLocations The array of migration files to be used.
   * @return The configured [Flyway] instance.
   */
  @JvmStatic
  fun create(
    dataSource: DataSource?,
    installedBy: String?,
    dbIdentifier: String?,
    vararg migrationFileLocations: String?,
  ): Flyway =
    create(
      dataSource,
      installedBy,
      dbIdentifier,
      BASELINE_VERSION,
      BASELINE_DESCRIPTION,
      BASELINE_ON_MIGRATION,
      *migrationFileLocations,
    )

  /**
   * Constructs a configured [Flyway] instance using the provided configuration.
   *
   * @param dataSource The [DataSource] used to connect to the database.
   * @param installedBy The name of the module performing the migration.
   * @param dbIdentifier The name of the database to be migrated. This is used to name the table to
   * hold the migration history for the database.
   * @param baselineVersion The version to tag an existing schema with when executing baseline.
   * @param baselineDescription The description to tag an existing schema with when executing
   * baseline.
   * @param baselineOnMigrate Whether to automatically call baseline when migrate is executed against
   * a non-empty schema with no schema history table.
   * @param migrationFileLocations The array of migration files to be used.
   * @return The configured [Flyway] instance.
   */
  @JvmStatic
  fun create(
    dataSource: DataSource?,
    installedBy: String?,
    dbIdentifier: String?,
    baselineVersion: String?,
    baselineDescription: String?,
    baselineOnMigrate: Boolean,
    vararg migrationFileLocations: String?,
  ): Flyway {
    val flywayConfiguration =
      Flyway
        .configure()
        .dataSource(dataSource)
        .baselineVersion(baselineVersion)
        .baselineDescription(baselineDescription)
        .baselineOnMigrate(baselineOnMigrate)
        .installedBy(installedBy)
        .table(String.format(MIGRATION_TABLE_FORMAT, dbIdentifier))
        .locations(*migrationFileLocations)

    // Setting the transactional lock to false allows us run queries outside transactions
    // without hanging. This enables creating indexes concurrently (i.e. without locking tables)
    flywayConfiguration.pluginRegister
      .getPlugin(PostgreSQLConfigurationExtension::class.java)
      .isTransactionalLock =
      false

    return flywayConfiguration.load()
  }
}
