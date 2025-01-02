/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.db.Database;
import io.airbyte.db.check.DatabaseMigrationCheck;
import io.airbyte.db.check.impl.JobsDatabaseAvailabilityCheck;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DatabaseCheckFactory;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.persistence.job.DefaultJobPersistence;
import io.airbyte.persistence.job.DefaultMetadataPersistence;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.MetadataPersistence;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import io.micronaut.flyway.FlywayConfigurationProperties;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micronaut bean factory for database-related singletons.
 */
@Factory
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "LineLength"})
public class DatabaseBeanFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String BASELINE_DESCRIPTION = "Baseline from file-based migration v1";
  private static final Boolean BASELINE_ON_MIGRATION = true;
  private static final String INSTALLED_BY = "ServerApp";

  @Singleton
  @Named("configDatabase")
  public Database configDatabase(@Named("config") final DSLContext dslContext) throws IOException {
    return new Database(unwrapContext(dslContext));
  }

  @Singleton
  @Named("configFlyway")
  public Flyway configFlyway(@Named("config") final FlywayConfigurationProperties configFlywayConfigurationProperties,
                             @Named("config") final DataSource configDataSource,
                             @Value("${airbyte.flyway.configs.minimum-migration-version}") final String baselineVersion) {
    return configFlywayConfigurationProperties.getFluentConfiguration()
        .dataSource(unwrapDataSource(configDataSource))
        .baselineVersion(baselineVersion)
        .baselineDescription(BASELINE_DESCRIPTION)
        .baselineOnMigrate(BASELINE_ON_MIGRATION)
        .installedBy(INSTALLED_BY)
        .table(String.format("airbyte_%s_migrations", "configs"))
        .load();
  }

  @Singleton
  @Named("jobsFlyway")
  public Flyway jobsFlyway(@Named("jobs") final FlywayConfigurationProperties jobsFlywayConfigurationProperties,
                           @Named("config") final DataSource jobsDataSource,
                           @Value("${airbyte.flyway.jobs.minimum-migration-version}") final String baselineVersion) {
    return jobsFlywayConfigurationProperties.getFluentConfiguration()
        .dataSource(unwrapDataSource(jobsDataSource))
        .baselineVersion(baselineVersion)
        .baselineDescription(BASELINE_DESCRIPTION)
        .baselineOnMigrate(BASELINE_ON_MIGRATION)
        .installedBy(INSTALLED_BY)
        .table(String.format("airbyte_%s_migrations", "jobs"))
        .load();
  }

  @Singleton
  public JobPersistence jobPersistence(@Named("configDatabase") final Database jobDatabase) {
    return new DefaultJobPersistence(jobDatabase);
  }

  @Singleton
  public MetadataPersistence metadataPersistence(@Named("configDatabase") final Database jobDatabase) {
    return new DefaultMetadataPersistence(jobDatabase);
  }

  @Singleton
  public PermissionPersistence permissionPersistence(@Named("configDatabase") final Database configDatabase) {
    return new PermissionPersistence(configDatabase);
  }

  @Singleton
  public StatePersistence statePersistence(@Named("configDatabase") final Database configDatabase) {
    return new StatePersistence(configDatabase);
  }

  @Singleton
  public UserPersistence userPersistence(@Named("configDatabase") final Database configDatabase) {
    return new UserPersistence(configDatabase);
  }

  @Singleton
  public OrganizationPersistence organizationPersistence(@Named("configDatabase") final Database configDatabase) {
    return new OrganizationPersistence(configDatabase);
  }

  @Singleton
  public WorkspacePersistence workspacePersistence(@Named("configDatabase") final Database configDatabase) {
    return new WorkspacePersistence(configDatabase);
  }

  @Singleton
  @Named("configsDatabaseMigrationCheck")
  public DatabaseMigrationCheck configsDatabaseMigrationCheck(@Named("config") final DSLContext dslContext,
                                                              @Named("configFlyway") final Flyway configsFlyway,
                                                              @Value("${airbyte.flyway.configs.minimum-migration-version}") final String configsDatabaseMinimumFlywayMigrationVersion,
                                                              @Value("${airbyte.flyway.configs.initialization-timeout-ms}") final Long configsDatabaseInitializationTimeoutMs) {
    log.info("Configs database configuration: {} {}", configsDatabaseMinimumFlywayMigrationVersion, configsDatabaseInitializationTimeoutMs);
    return DatabaseCheckFactory
        .createConfigsDatabaseMigrationCheck(unwrapContext(dslContext), configsFlyway, configsDatabaseMinimumFlywayMigrationVersion,
            configsDatabaseInitializationTimeoutMs);
  }

  @Singleton
  @Named("jobsDatabaseMigrationCheck")
  public DatabaseMigrationCheck jobsDatabaseMigrationCheck(@Named("config") final DSLContext dslContext,
                                                           @Named("jobsFlyway") final Flyway jobsFlyway,
                                                           @Value("${airbyte.flyway.jobs.minimum-migration-version}") final String jobsDatabaseMinimumFlywayMigrationVersion,
                                                           @Value("${airbyte.flyway.jobs.initialization-timeout-ms}") final Long jobsDatabaseInitializationTimeoutMs) {
    return DatabaseCheckFactory
        .createJobsDatabaseMigrationCheck(unwrapContext(dslContext), jobsFlyway, jobsDatabaseMinimumFlywayMigrationVersion,
            jobsDatabaseInitializationTimeoutMs);
  }

  @Singleton
  @Named("jobsDatabaseAvailabilityCheck")
  public JobsDatabaseAvailabilityCheck jobsDatabaseAvailabilityCheck(@Named("config") final DSLContext dslContext) {
    return new JobsDatabaseAvailabilityCheck(unwrapContext(dslContext), DatabaseConstants.DEFAULT_ASSERT_DATABASE_TIMEOUT_MS);
  }

  @Singleton
  public StreamResetPersistence streamResetPersistence(@Named("configDatabase") final Database configDatabase) {
    return new StreamResetPersistence(configDatabase);
  }

  @Singleton
  @Named("unwrappedConfig")
  public DSLContext unwrappedConfigDslContext(@Named("config") final DSLContext dslContext) {
    return unwrapContext(dslContext);
  }

  // Micronaut-data wraps the injected data sources with transactional semantics, which don't respect
  // our jooq operations and error out. If we inject an unwrapped one, it will be re-wrapped. So we
  // manually unwrap them.
  static DataSource unwrapDataSource(final DataSource dataSource) {
    return ((DelegatingDataSource) dataSource).getTargetDataSource();
  }

  // For some reason, it won't let us provide an unwrapped dsl context as a bean, so we manually
  // unwrap the data source here as well.
  static DSLContext unwrapContext(final DSLContext context) {
    final var datasource = ((DataSourceConnectionProvider) context.configuration().connectionProvider()).dataSource();

    return DSLContextFactory.create(unwrapDataSource(datasource), SQLDialect.POSTGRES);
  }

}
