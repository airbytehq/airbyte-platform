/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config

import io.airbyte.commons.constants.WorkerConstants.KubeConstants
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.DataSourceUnwrapper
import io.airbyte.db.Database
import io.airbyte.db.check.DatabaseMigrationCheck
import io.airbyte.db.factory.DatabaseCheckFactory
import io.airbyte.persistence.job.DbPrune
import io.airbyte.persistence.job.DefaultJobPersistence
import io.airbyte.persistence.job.DefaultMetadataPersistence
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.MetadataPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.flyway.FlywayConfigurationProperties
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import javax.sql.DataSource

val log = KotlinLogging.logger {}
private const val BASELINE_DESCRIPTION = "Baseline from file-based migration v1"
private const val BASELINE_ON_MIGRATION = true
private const val INSTALLED_BY = "AirbyteCron"

@Factory
class BeanFactory {
  @Singleton
  @Named("replicationNotStartedTimeout")
  fun notStartedTimeout(): Duration {
    val sourcePodTimeoutMs = KubeConstants.FULL_POD_TIMEOUT
    val destPodTimeoutMs = KubeConstants.FULL_POD_TIMEOUT
    val orchestratorInitPodTimeoutMs = KubeConstants.INIT_CONTAINER_STARTUP_TIMEOUT

    // Calculate total timeout by summing pod timeouts and applying a 1.2 multiplier
    return (sourcePodTimeoutMs + destPodTimeoutMs + orchestratorInitPodTimeoutMs)
      .multipliedBy(12) // multipliedBy only takes whole numbers
      .dividedBy(10)
  }

  @Singleton
  fun workspaceHelper(
    jobPersistence: JobPersistence,
    connectionService: ConnectionService,
    sourceService: SourceService,
    destinationService: DestinationService,
    operationService: OperationService,
    workspaceService: WorkspaceService,
  ): WorkspaceHelper = WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService)

  @Singleton
  fun timeProvider(): Function1<ZoneId, OffsetDateTime> = { zone: ZoneId -> OffsetDateTime.now(zone) }

  @Singleton
  @Named("configDatabase")
  fun configDatabase(
    @Named("config") dslContext: DSLContext,
  ): Database = Database(DataSourceUnwrapper.unwrapContext(dslContext))

  @Singleton
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  @Named("jobsDatabase")
  fun jobsDatabase(
    @Named("jobs") dslContext: DSLContext,
  ): Database = Database(DataSourceUnwrapper.unwrapContext(dslContext))

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
  fun configFlyway(
    @Named("config") configFlywayConfigurationProperties: FlywayConfigurationProperties,
    @Named("config") configDataSource: DataSource,
    @Value("\${airbyte.flyway.configs.minimum-migration-version}") baselineVersion: String,
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
   * Database migration check.
   *
   * @param dslContext db context
   * @param configsFlyway config for flyway
   * @param configsDatabaseMinimumFlywayMigrationVersion minimum flyway migration version
   * @param configsDatabaseInitializationTimeoutMs timeout
   * @return check for database migration
   */
  @Singleton
  @Named("configsDatabaseMigrationCheck")
  fun configsDatabaseMigrationCheck(
    @Named("config") dslContext: DSLContext,
    @Named("configFlyway") configsFlyway: Flyway,
    @Value("\${airbyte.flyway.configs.minimum-migration-version}") configsDatabaseMinimumFlywayMigrationVersion: String,
    @Value("\${airbyte.flyway.configs.initialization-timeout-ms}") configsDatabaseInitializationTimeoutMs: Long,
  ): DatabaseMigrationCheck {
    log.info {
      "Configs database configuration: $configsDatabaseMinimumFlywayMigrationVersion $configsDatabaseInitializationTimeoutMs"
    }

    return DatabaseCheckFactory
      .createConfigsDatabaseMigrationCheck(
        DataSourceUnwrapper.unwrapContext(dslContext),
        configsFlyway,
        configsDatabaseMinimumFlywayMigrationVersion,
        configsDatabaseInitializationTimeoutMs,
      )
  }

  @Singleton
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  fun streamResetPersistence(
    @Named("configDatabase") configDatabase: Database,
  ): StreamResetPersistence = StreamResetPersistence(configDatabase)

  @Singleton
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  fun jobPersistence(
    @Named("jobsDatabase") jobDatabase: Database,
  ): JobPersistence = DefaultJobPersistence(jobDatabase)

  @Singleton
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  fun metadataPersistence(
    @Named("jobsDatabase") jobDatabase: Database,
  ): MetadataPersistence = DefaultMetadataPersistence(jobDatabase)

  @Singleton
  @Named("dbPrune")
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  fun dbPrune(
    @Named("jobsDatabase") jobDatabase: Database,
  ): DbPrune = DbPrune(jobDatabase)
}
