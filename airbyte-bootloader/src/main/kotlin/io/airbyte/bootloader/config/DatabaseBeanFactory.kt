/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.config

import io.airbyte.commons.resources.Resources
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.shared.DataSourceUnwrapper
import io.airbyte.db.Database
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.factory.DatabaseCheckFactory
import io.airbyte.db.init.DatabaseInitializer
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator
import io.airbyte.persistence.job.DefaultJobPersistence
import io.airbyte.persistence.job.DefaultMetadataPersistence
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.MetadataPersistence
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.flyway.FlywayConfigurationProperties
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.flywaydb.core.Flyway
import org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension
import org.jooq.DSLContext
import javax.sql.DataSource

/**
 * Micronaut bean factory for database-related singletons.
 */
@Factory
class DatabaseBeanFactory {
  @Singleton
  @Named("configDatabase")
  fun configDatabase(
    @Named("config") dslContext: DSLContext,
  ): Database = Database(DataSourceUnwrapper.unwrapContext(dslContext))

  @Singleton
  @Named("jobsDatabase")
  fun jobsDatabase(
    @Named("jobs") dslContext: DSLContext,
  ): Database = Database(DataSourceUnwrapper.unwrapContext(dslContext))

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
  fun configFlyway(
    @Named("config") configFlywayConfigurationProperties: FlywayConfigurationProperties,
    @Named("config") configDataSource: DataSource,
    @Value("\${airbyte.bootloader.migration-baseline-version}") baselineVersion: String,
  ): Flyway =
    configFlywayConfigurationProperties.fluentConfiguration
      .dataSource(DataSourceUnwrapper.unwrapDataSource(configDataSource))
      .baselineVersion(baselineVersion)
      .baselineDescription(BASELINE_DESCRIPTION)
      .baselineOnMigrate(BASELINE_ON_MIGRATION)
      .installedBy(INSTALLED_BY)
      .table(String.format("airbyte_%s_migrations", "configs"))
      .load()

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
  fun jobsFlyway(
    @Named("jobs") jobsFlywayConfigurationProperties: FlywayConfigurationProperties,
    @Named("jobs") jobsDataSource: DataSource,
    @Value("\${airbyte.bootloader.migration-baseline-version}") baselineVersion: String,
  ): Flyway {
    val flywayConfiguration =
      jobsFlywayConfigurationProperties.fluentConfiguration
        .dataSource(DataSourceUnwrapper.unwrapDataSource(jobsDataSource))
        .baselineVersion(baselineVersion)
        .baselineDescription(BASELINE_DESCRIPTION)
        .baselineOnMigrate(BASELINE_ON_MIGRATION)
        .installedBy(INSTALLED_BY)
        .table(String.format("airbyte_%s_migrations", "jobs"))

    // Setting the transactional lock to false allows us run queries outside transactions
    // without hanging. This enables creating indexes concurrently (i.e. without locking tables)
    flywayConfiguration.pluginRegister
      .getPlugin(PostgreSQLConfigurationExtension::class.java)
      .isTransactionalLock =
      false

    return flywayConfiguration.load()
  }

  @Singleton
  fun jobPersistence(
    @Named("jobsDatabase") jobDatabase: Database?,
  ): JobPersistence = DefaultJobPersistence(jobDatabase)

  @Singleton
  fun metadataPersistence(
    @Named("jobsDatabase") jobDatabase: Database?,
  ): MetadataPersistence = DefaultMetadataPersistence(jobDatabase)

  @Singleton
  @Named("configsDatabaseInitializer")
  fun configsDatabaseInitializer(
    @Named("config") configsDslContext: DSLContext,
    @Value("\${airbyte.flyway.configs.initialization-timeout-ms}") configsDatabaseInitializationTimeoutMs: Long,
  ): DatabaseInitializer =
    DatabaseCheckFactory.createConfigsDatabaseInitializer(
      DataSourceUnwrapper.unwrapContext(configsDslContext),
      configsDatabaseInitializationTimeoutMs,
      Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH),
    )

  @Singleton
  @Named("jobsDatabaseInitializer")
  fun jobsDatabaseInitializer(
    @Named("jobs") jobsDslContext: DSLContext,
    @Value("\${airbyte.flyway.jobs.initialization-timeout-ms}") jobsDatabaseInitializationTimeoutMs: Long,
  ): DatabaseInitializer =
    DatabaseCheckFactory.createJobsDatabaseInitializer(
      DataSourceUnwrapper.unwrapContext(jobsDslContext),
      jobsDatabaseInitializationTimeoutMs,
      Resources.read(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH),
    )

  @Singleton
  @Named("jobsDatabaseAvailabilityCheck")
  fun jobsDatabaseAvailabilityCheck(
    @Named("jobs") dslContext: DSLContext,
  ): JobsDatabaseAvailabilityCheck =
    JobsDatabaseAvailabilityCheck(DataSourceUnwrapper.unwrapContext(dslContext), DatabaseConstants.DEFAULT_ASSERT_DATABASE_TIMEOUT_MS)

  @Singleton
  @Named("configsDatabaseMigrator")
  fun configsDatabaseMigrator(
    @Named("configDatabase") configDatabase: Database,
    @Named("configFlyway") configFlyway: Flyway,
  ): DatabaseMigrator = ConfigsDatabaseMigrator(configDatabase, configFlyway)

  @Singleton
  @Named("jobsDatabaseMigrator")
  fun jobsDatabaseMigrator(
    @Named("jobsDatabase") jobsDatabase: Database,
    @Named("jobsFlyway") jobsFlyway: Flyway,
  ): DatabaseMigrator = JobsDatabaseMigrator(jobsDatabase, jobsFlyway)

  @Singleton
  fun userPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): UserPersistence = UserPersistence(configDatabase)

  @Singleton
  fun organizationPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): OrganizationPersistence = OrganizationPersistence(configDatabase)

  @Singleton
  fun workspacePersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): WorkspacePersistence = WorkspacePersistence(configDatabase)

  companion object {
    private const val BASELINE_DESCRIPTION = "Baseline from file-based migration v1"
    private const val BASELINE_ON_MIGRATION = true
    private const val INSTALLED_BY = "BootloaderApp"
  }
}
