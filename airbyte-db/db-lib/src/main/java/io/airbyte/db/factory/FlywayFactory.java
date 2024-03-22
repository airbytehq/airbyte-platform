/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension;

/**
 * Temporary factory class that provides convenience methods for creating a {@link Flyway}
 * instances. This class will be removed once the project has been converted to leverage an
 * application framework to manage the creation and injection of {@link Flyway} objects.
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class FlywayFactory {

  static final String MIGRATION_TABLE_FORMAT = "airbyte_%s_migrations";

  // Constants for Flyway baseline. See here for details:
  // https://flywaydb.org/documentation/command/baseline
  static final String BASELINE_VERSION = "0.29.0.001";
  static final String BASELINE_DESCRIPTION = "Baseline from file-based migration v1";
  static final boolean BASELINE_ON_MIGRATION = true;

  /**
   * Constructs a configured {@link Flyway} instance using the provided configuration.
   *
   * @param dataSource The {@link DataSource} used to connect to the database.
   * @param installedBy The name of the module performing the migration.
   * @param dbIdentifier The name of the database to be migrated. This is used to name the table to
   *        hold the migration history for the database.
   * @param migrationFileLocations The array of migration files to be used.
   * @return The configured {@link Flyway} instance.
   */
  public static Flyway create(final DataSource dataSource,
                              final String installedBy,
                              final String dbIdentifier,
                              final String... migrationFileLocations) {
    return create(dataSource,
        installedBy,
        dbIdentifier,
        BASELINE_VERSION,
        BASELINE_DESCRIPTION,
        BASELINE_ON_MIGRATION,
        migrationFileLocations);
  }

  /**
   * Constructs a configured {@link Flyway} instance using the provided configuration.
   *
   * @param dataSource The {@link DataSource} used to connect to the database.
   * @param installedBy The name of the module performing the migration.
   * @param dbIdentifier The name of the database to be migrated. This is used to name the table to
   *        hold the migration history for the database.
   * @param baselineVersion The version to tag an existing schema with when executing baseline.
   * @param baselineDescription The description to tag an existing schema with when executing
   *        baseline.
   * @param baselineOnMigrate Whether to automatically call baseline when migrate is executed against
   *        a non-empty schema with no schema history table.
   * @param migrationFileLocations The array of migration files to be used.
   * @return The configured {@link Flyway} instance.
   */
  public static Flyway create(final DataSource dataSource,
                              final String installedBy,
                              final String dbIdentifier,
                              final String baselineVersion,
                              final String baselineDescription,
                              final boolean baselineOnMigrate,
                              final String... migrationFileLocations) {
    final var flywayConfiguration = Flyway.configure()
        .dataSource(dataSource)
        .baselineVersion(baselineVersion)
        .baselineDescription(baselineDescription)
        .baselineOnMigrate(baselineOnMigrate)
        .installedBy(installedBy)
        .table(String.format(MIGRATION_TABLE_FORMAT, dbIdentifier))
        .locations(migrationFileLocations);

    // Setting the transactional lock to false allows us run queries outside transactions
    // without hanging. This enables creating indexes concurrently (i.e. without locking tables)
    flywayConfiguration.getPluginRegister()
        .getPlugin(PostgreSQLConfigurationExtension.class)
        .setTransactionalLock(false);

    return flywayConfiguration.load();
  }

}
