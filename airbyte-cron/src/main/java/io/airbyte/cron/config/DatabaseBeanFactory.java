/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config;

import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.data.services.shared.DataSourceUnwrapper;
import io.airbyte.db.Database;
import io.airbyte.db.check.DatabaseMigrationCheck;
import io.airbyte.db.factory.DatabaseCheckFactory;
import io.airbyte.persistence.job.DefaultJobPersistence;
import io.airbyte.persistence.job.DefaultMetadataPersistence;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.MetadataPersistence;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.flyway.FlywayConfigurationProperties;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micronaut bean factory for database-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class DatabaseBeanFactory {

  private static final Logger log = LoggerFactory.getLogger(DatabaseBeanFactory.class);
  private static final String BASELINE_DESCRIPTION = "Baseline from file-based migration v1";
  private static final Boolean BASELINE_ON_MIGRATION = true;
  private static final String INSTALLED_BY = "AirbyteCron";

  @Singleton
  @Named("configDatabase")
  public Database configDatabase(@Named("config") final DSLContext dslContext) {
    return new Database(DataSourceUnwrapper.unwrapContext(dslContext));
  }

  @Singleton
  @Requires(env = EnvConstants.CONTROL_PLANE)
  @Named("jobsDatabase")
  public Database jobsDatabase(@Named("jobs") final DSLContext dslContext) {
    return new Database(DataSourceUnwrapper.unwrapContext(dslContext));
  }

  /**
   * Flyway config.
   *
   * @param configFlywayConfigurationProperties flyway configuration
   * @param configDataSource configs db source
   * @param baselineVersion baseline version
   * @return flyway config
   */
  @Singleton
  @Named("configFlyway")
  public Flyway configFlyway(@Named("config") final FlywayConfigurationProperties configFlywayConfigurationProperties,
                             @Named("config") final DataSource configDataSource,
                             @Value("${airbyte.flyway.configs.minimum-migration-version}") final String baselineVersion) {
    return configFlywayConfigurationProperties.getFluentConfiguration()
        .dataSource(DataSourceUnwrapper.unwrapDataSource(configDataSource))
        .baselineVersion(baselineVersion)
        .baselineDescription(BASELINE_DESCRIPTION)
        .baselineOnMigrate(BASELINE_ON_MIGRATION)
        .installedBy(INSTALLED_BY)
        .table(String.format("airbyte_%s_migrations", "configs"))
        .load();
  }

  /**
   * Database migration check.
   *
   * @param dslContext db context
   * @param configsFlyway config for flyway
   * @param configsDatabaseMinimumFlywayMigrationVersion minimum flyway migration version
   * @param configsDatabaseInitializationTimeoutMs timeout
   * @return check for database migration
   */
  @SuppressWarnings("LineLength")
  @Singleton
  @Named("configsDatabaseMigrationCheck")
  public DatabaseMigrationCheck configsDatabaseMigrationCheck(@Named("config") final DSLContext dslContext,
                                                              @Named("configFlyway") final Flyway configsFlyway,
                                                              @Value("${airbyte.flyway.configs.minimum-migration-version}") final String configsDatabaseMinimumFlywayMigrationVersion,
                                                              @Value("${airbyte.flyway.configs.initialization-timeout-ms}") final Long configsDatabaseInitializationTimeoutMs) {
    log.info("Configs database configuration: {} {}", configsDatabaseMinimumFlywayMigrationVersion, configsDatabaseInitializationTimeoutMs);
    return DatabaseCheckFactory
        .createConfigsDatabaseMigrationCheck(
            DataSourceUnwrapper.unwrapContext(dslContext),
            configsFlyway,
            configsDatabaseMinimumFlywayMigrationVersion,
            configsDatabaseInitializationTimeoutMs);
  }

  @Singleton
  @Requires(env = EnvConstants.CONTROL_PLANE)
  public StreamResetPersistence streamResetPersistence(@Named("configDatabase") final Database configDatabase) {
    return new StreamResetPersistence(configDatabase);
  }

  @Singleton
  @Requires(env = EnvConstants.CONTROL_PLANE)
  public JobPersistence jobPersistence(@Named("jobsDatabase") final Database jobDatabase) {
    return new DefaultJobPersistence(jobDatabase);
  }

  @Singleton
  @Requires(env = EnvConstants.CONTROL_PLANE)
  public MetadataPersistence metadataPersistence(@Named("jobsDatabase") final Database jobDatabase) {
    return new DefaultMetadataPersistence(jobDatabase);
  }

}
