/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.db.Database
import io.airbyte.db.check.DatabaseMigrationCheck
import io.airbyte.persistence.job.DefaultJobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.impl.DataSourceConnectionProvider
import javax.sql.DataSource

/**
 * Test factory used to replace/override beans for unit tests.
 */
@Factory
class TestFactory {
  @Singleton
  @Replaces(TemporalClient::class)
  fun temporalClient(): TemporalClient = mockk()

  @Singleton
  @Replaces(named = "configsDatabaseMigrationCheck")
  @Named("configsDatabaseMigrationCheck")
  fun configsDatabaseMigrationCheck(): DatabaseMigrationCheck = mockk(relaxed = true)

  @Singleton
  @Replaces(named = "jobsDatabaseMigrationCheck")
  @Named("jobsDatabaseMigrationCheck")
  fun jobsDatabaseMigrationCheck(): DatabaseMigrationCheck = mockk(relaxed = true)

  @Singleton
  @Replaces(DefaultJobPersistence::class)
  fun defaultJobPersistence(): DefaultJobPersistence = mockk()

  @Singleton
  @Replaces(EventRunner::class)
  fun eventRunner(): EventRunner = mockk()

  @Singleton
  @Replaces(OAuthConfigSupplier::class)
  fun oauthConfigSupplier(): OAuthConfigSupplier = mockk()

  @Singleton
  @Replaces(TrackingClient::class)
  fun trackingClient(): TrackingClient = mockk()

  @Singleton
  @Replaces(value = DSLContext::class, named = "config")
  @Named("config")
  fun configDslContext(
    @Named("config") dataSource: DataSource,
  ): DSLContext = createMockDslContext(dataSource)

  @Singleton
  @Replaces(value = DSLContext::class, named = "jobs")
  @Named("jobs")
  fun jobsDslContext(
    @Named("jobs") dataSource: DataSource,
  ): DSLContext = createMockDslContext(dataSource)

  @Singleton
  @Replaces(value = DSLContext::class, named = "local-secrets")
  @Named("local-secrets")
  fun localSecretsDslContext(
    @Named("local-secrets") dataSource: DataSource,
  ): DSLContext = createMockDslContext(dataSource)

  @Singleton
  @Replaces(Database::class)
  @Named("configDatabase")
  fun mmDatabase(): Database = mockk()

  private fun createMockDslContext(dataSource: DataSource): DSLContext {
    val configuration = mockk<Configuration>()
    val connectionProvider = mockk<DataSourceConnectionProvider>()
    val dslContext = mockk<DSLContext>()
    every { connectionProvider.dataSource() } returns dataSource
    every { configuration.connectionProvider() } returns connectionProvider
    every { dslContext.configuration() } returns configuration
    return dslContext
  }
}
