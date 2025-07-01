/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.config

import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.db.Database
import io.airbyte.db.check.DatabaseMigrationCheck
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.factory.DatabaseCheckFactory
import io.airbyte.persistence.job.DefaultJobPersistence
import io.airbyte.persistence.job.JobPersistence
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
 * This factory (and the related [HelperBeanFactory]) are here to provide object
 * instantiation logic for classes that are required to interact with the config db
 * to check permissions.
 *
 * At the time of this comment, this is because the [RoleResolver] contains a
 * dependency chain that relies on various persistence classes (though the [WorkspaceHelper] class)
 * that need to be available. The [RoleResolver] was added a dependency in order to
 * authorize calls to the api from dataplanes.
 *
 * When workload-api-server is merged into airbyte-server, this factory will become
 * redundant, as the airbyte-server already has this logic itself, and this code
 * can be deleted.
 */
@Factory
class DatabaseBeanFactory {
  @Singleton
  @Named("configDatabase")
  fun configDatabase(
    @Named("config") dslContext: DSLContext,
  ): Database = Database(unwrapContext(dslContext))

  @Singleton
  fun jobPersistence(
    @Named("configDatabase") jobDatabase: Database?,
  ): JobPersistence = DefaultJobPersistence(jobDatabase)

  @Singleton
  @Named("userPersistence")
  fun userPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): UserPersistence = UserPersistence(configDatabase)

  @Singleton
  fun workspacePersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): WorkspacePersistence = WorkspacePersistence(configDatabase)

  @Singleton
  fun organizationPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): OrganizationPersistence = OrganizationPersistence(configDatabase)

  @Singleton
  fun permissionPersistence(
    @Named("configDatabase") configDatabase: Database?,
  ): PermissionPersistence = PermissionPersistence(configDatabase)

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
