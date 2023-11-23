/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.config;

import static io.airbyte.data.services.shared.DataSourceUnwrapper.unwrapContext;
import static io.airbyte.data.services.shared.DataSourceUnwrapper.unwrapDataSource;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.HealthCheckService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.db.Database;
import io.airbyte.db.check.impl.JobsDatabaseAvailabilityCheck;
import io.airbyte.db.factory.DatabaseCheckFactory;
import io.airbyte.db.init.DatabaseInitializer;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.DatabaseMigrator;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator;
import io.airbyte.persistence.job.DefaultJobPersistence;
import io.airbyte.persistence.job.DefaultMetadataPersistence;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.MetadataPersistence;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Value;
import io.micronaut.flyway.FlywayConfigurationProperties;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;

/**
 * Micronaut bean factory for database-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class DatabaseBeanFactory {

  private static final String BASELINE_DESCRIPTION = "Baseline from file-based migration v1";
  private static final Boolean BASELINE_ON_MIGRATION = true;
  private static final String INSTALLED_BY = "BootloaderApp";

  @Singleton
  @Named("configDatabase")
  public Database configDatabase(@Named("config") final DSLContext dslContext) throws IOException {
    return new Database(unwrapContext(dslContext));
  }

  @Singleton
  @Named("jobsDatabase")
  public Database jobsDatabase(@Named("jobs") final DSLContext dslContext) throws IOException {
    return new Database(unwrapContext(dslContext));
  }

  /**
   * Flyway configs db singleton.
   *
   * @param configFlywayConfigurationProperties config db flyway configuration
   * @param configDataSource configs db data source
   * @param baselineVersion baseline migration version
   * @return flyway
   */
  @Singleton
  @Named("configFlyway")
  public Flyway configFlyway(@Named("config") final FlywayConfigurationProperties configFlywayConfigurationProperties,
                             @Named("config") final DataSource configDataSource,
                             @Value("${airbyte.bootloader.migration-baseline-version}") final String baselineVersion) {
    return configFlywayConfigurationProperties.getFluentConfiguration()
        .dataSource(unwrapDataSource(configDataSource))
        .baselineVersion(baselineVersion)
        .baselineDescription(BASELINE_DESCRIPTION)
        .baselineOnMigrate(BASELINE_ON_MIGRATION)
        .installedBy(INSTALLED_BY)
        .table(String.format("airbyte_%s_migrations", "configs"))
        .load();
  }

  /**
   * Flyway jobs db singleton.
   *
   * @param jobsFlywayConfigurationProperties jobs flyway configuration
   * @param jobsDataSource jobs data source
   * @param baselineVersion base line migration version
   * @return flyway
   */
  @Singleton
  @Named("jobsFlyway")
  public Flyway jobsFlyway(@Named("jobs") final FlywayConfigurationProperties jobsFlywayConfigurationProperties,
                           @Named("jobs") final DataSource jobsDataSource,
                           @Value("${airbyte.bootloader.migration-baseline-version}") final String baselineVersion) {
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
  @Replaces(ConfigRepository.class)
  public ConfigRepository configRepository(final ActorDefinitionService actorDefinitionService,
                                           final CatalogService catalogService,
                                           final ConnectionService connectionService,
                                           final ConnectorBuilderService connectorBuilderService,
                                           final DestinationService destinationService,
                                           final HealthCheckService healthCheckService,
                                           final OAuthService oauthService,
                                           final OperationService operationService,
                                           final OrganizationService organizationService,
                                           final SourceService sourceService,
                                           final WorkspaceService workspaceService) {
    return new ConfigRepository(
        actorDefinitionService,
        catalogService,
        connectionService,
        connectorBuilderService,
        destinationService,
        healthCheckService,
        oauthService,
        operationService,
        organizationService,
        sourceService,
        workspaceService);
  }

  @Singleton
  public JobPersistence jobPersistence(@Named("jobsDatabase") final Database jobDatabase) {
    return new DefaultJobPersistence(jobDatabase);
  }

  @Singleton
  public MetadataPersistence metadataPersistence(@Named("jobsDatabase") final Database jobDatabase) {
    return new DefaultMetadataPersistence(jobDatabase);
  }

  @SuppressWarnings("LineLength")
  @Singleton
  @Named("configsDatabaseInitializer")
  public DatabaseInitializer configsDatabaseInitializer(@Named("config") final DSLContext configsDslContext,
                                                        @Value("${airbyte.flyway.configs.initialization-timeout-ms}") final Long configsDatabaseInitializationTimeoutMs)
      throws IOException {
    return DatabaseCheckFactory.createConfigsDatabaseInitializer(unwrapContext(configsDslContext),
        configsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH));
  }

  @SuppressWarnings("LineLength")
  @Singleton
  @Named("jobsDatabaseInitializer")
  public DatabaseInitializer jobsDatabaseInitializer(@Named("jobs") final DSLContext jobsDslContext,
                                                     @Value("${airbyte.flyway.jobs.initialization-timeout-ms}") final Long jobsDatabaseInitializationTimeoutMs)
      throws IOException {
    return DatabaseCheckFactory.createJobsDatabaseInitializer(unwrapContext(jobsDslContext),
        jobsDatabaseInitializationTimeoutMs, MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH));
  }

  @Singleton
  @Named("jobsDatabaseAvailabilityCheck")
  public JobsDatabaseAvailabilityCheck jobsDatabaseAvailabilityCheck(@Named("jobs") final DSLContext dslContext) {
    return new JobsDatabaseAvailabilityCheck(unwrapContext(dslContext), DatabaseConstants.DEFAULT_ASSERT_DATABASE_TIMEOUT_MS);
  }

  @Singleton
  @Named("configsDatabaseMigrator")
  public DatabaseMigrator configsDatabaseMigrator(@Named("configDatabase") final Database configDatabase,
                                                  @Named("configFlyway") final Flyway configFlyway) {
    return new ConfigsDatabaseMigrator(configDatabase, configFlyway);
  }

  @Singleton
  @Named("jobsDatabaseMigrator")
  public DatabaseMigrator jobsDatabaseMigrator(@Named("jobsDatabase") final Database jobsDatabase,
                                               @Named("jobsFlyway") final Flyway jobsFlyway) {
    return new JobsDatabaseMigrator(jobsDatabase, jobsFlyway);
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

}
