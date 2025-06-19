/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.db.Database
import io.airbyte.db.check.DatabaseMigrationCheck
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.factory.DatabaseCheckFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.persistence.job.DefaultJobPersistence
import io.airbyte.persistence.job.DefaultMetadataPersistence
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.MetadataPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.flyway.FlywayConfigurationProperties
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DataSourceConnectionProvider
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

/**
 * Micronaut bean factory for database-related singletons.
 */
@Factory
class DatabaseBeanFactory {
  @Singleton
  @Named("configDatabase")
  fun configDatabase(
    @Named("config") dslContext: DSLContext,
  ): Database = Database(unwrapContext(dslContext))

  @Singleton
  @Named("configFlyway")
  fun configFlyway(
    @Named("config") configFlywayConfigurationProperties: FlywayConfigurationProperties,
    @Named("config") configDataSource: DataSource,
    @Value("\${airbyte.flyway.configs.minimum-migration-version}") baselineVersion: String?,
  ): Flyway =
    configFlywayConfigurationProperties.fluentConfiguration
      .dataSource(unwrapDataSource(configDataSource))
      .baselineVersion(baselineVersion)
      .baselineDescription(BASELINE_DESCRIPTION)
      .baselineOnMigrate(BASELINE_ON_MIGRATION)
      .installedBy(INSTALLED_BY)
      .table(String.format("airbyte_%s_migrations", "configs"))
      .load()

  @Singleton
  @Named("jobsFlyway")
  fun jobsFlyway(
    @Named("jobs") jobsFlywayConfigurationProperties: FlywayConfigurationProperties,
    @Named("config") jobsDataSource: DataSource,
    @Value("\${airbyte.flyway.jobs.minimum-migration-version}") baselineVersion: String?,
  ): Flyway =
    jobsFlywayConfigurationProperties.fluentConfiguration
      .dataSource(unwrapDataSource(jobsDataSource))
      .baselineVersion(baselineVersion)
      .baselineDescription(BASELINE_DESCRIPTION)
      .baselineOnMigrate(BASELINE_ON_MIGRATION)
      .installedBy(INSTALLED_BY)
      .table(String.format("airbyte_%s_migrations", "jobs"))
      .load()

  @Singleton
  fun jobPersistence(
    @Named("configDatabase") jobDatabase: Database?,
  ): JobPersistence = DefaultJobPersistence(jobDatabase)

  @Singleton
  fun metadataPersistence(
    @Named("configDatabase") jobDatabase: Database?,
  ): MetadataPersistence = DefaultMetadataPersistence(jobDatabase)

  @Singleton
  fun permissionPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): PermissionPersistence = PermissionPersistence(configDatabase)

  @Singleton
  fun statePersistence(
    @Named("configDatabase") configDatabase: Database?,
    connectionServiceJooqImpl: ConnectionServiceJooqImpl,
  ): StatePersistence = StatePersistence(configDatabase, connectionServiceJooqImpl)

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

  @Singleton
  @Named("configsDatabaseMigrationCheck")
  fun configsDatabaseMigrationCheck(
    @Named("config") dslContext: DSLContext,
    @Named("configFlyway") configsFlyway: Flyway,
    @Value("\${airbyte.flyway.configs.minimum-migration-version}") configsDatabaseMinimumFlywayMigrationVersion: String,
    @Value("\${airbyte.flyway.configs.initialization-timeout-ms}") configsDatabaseInitializationTimeoutMs: Long,
  ): DatabaseMigrationCheck {
    log.info { "${"Configs database configuration: {} {}"} $configsDatabaseMinimumFlywayMigrationVersion $configsDatabaseInitializationTimeoutMs" }
    return DatabaseCheckFactory
      .createConfigsDatabaseMigrationCheck(
        unwrapContext(dslContext),
        configsFlyway,
        configsDatabaseMinimumFlywayMigrationVersion,
        configsDatabaseInitializationTimeoutMs,
      )
  }

  @Singleton
  @Named("jobsDatabaseMigrationCheck")
  fun jobsDatabaseMigrationCheck(
    @Named("config") dslContext: DSLContext,
    @Named("jobsFlyway") jobsFlyway: Flyway,
    @Value("\${airbyte.flyway.jobs.minimum-migration-version}") jobsDatabaseMinimumFlywayMigrationVersion: String,
    @Value("\${airbyte.flyway.jobs.initialization-timeout-ms}") jobsDatabaseInitializationTimeoutMs: Long,
  ): DatabaseMigrationCheck =
    DatabaseCheckFactory
      .createJobsDatabaseMigrationCheck(
        unwrapContext(dslContext),
        jobsFlyway,
        jobsDatabaseMinimumFlywayMigrationVersion,
        jobsDatabaseInitializationTimeoutMs,
      )

  @Singleton
  @Named("jobsDatabaseAvailabilityCheck")
  fun jobsDatabaseAvailabilityCheck(
    @Named("config") dslContext: DSLContext,
  ): JobsDatabaseAvailabilityCheck = JobsDatabaseAvailabilityCheck(unwrapContext(dslContext), DatabaseConstants.DEFAULT_ASSERT_DATABASE_TIMEOUT_MS)

  @Singleton
  fun streamResetPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): StreamResetPersistence = StreamResetPersistence(configDatabase)

  @Singleton
  @Named("unwrappedConfig")
  fun unwrappedConfigDslContext(
    @Named("config") dslContext: DSLContext,
  ): DSLContext = unwrapContext(dslContext)

  companion object {
    private const val BASELINE_DESCRIPTION = "Baseline from file-based migration v1"
    private const val BASELINE_ON_MIGRATION = true
    private const val INSTALLED_BY = "ServerApp"

    // Micronaut-data wraps the injected data sources with transactional semantics, which don't respect
    // our jooq operations and error out. If we inject an unwrapped one, it will be re-wrapped. So we
    // manually unwrap them.
    fun unwrapDataSource(dataSource: DataSource): DataSource = (dataSource as DelegatingDataSource).targetDataSource

    // For some reason, it won't let us provide an unwrapped dsl context as a bean, so we manually
    // unwrap the data source here as well.
    fun unwrapContext(context: DSLContext): DSLContext {
      val datasource = (context.configuration().connectionProvider() as DataSourceConnectionProvider).dataSource()

      return DSLContextFactory.create(unwrapDataSource(datasource), SQLDialect.POSTGRES)
    }
  }
}
