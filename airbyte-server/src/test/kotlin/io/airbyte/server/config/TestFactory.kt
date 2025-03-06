/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.impl.DataSourceConnectionProvider
import org.mockito.Mockito
import javax.sql.DataSource

/**
 * Test factory used to replace/override beans for unit tests.
 */
@Factory
class TestFactory {
  @Singleton
  @Replaces(TemporalClient::class)
  fun temporalClient(): TemporalClient = Mockito.mock(TemporalClient::class.java)

  @Singleton
  @Replaces(named = "configsDatabaseMigrationCheck")
  @Named("configsDatabaseMigrationCheck")
  fun configsDatabaseMigrationCheck(): DatabaseMigrationCheck = Mockito.mock(DatabaseMigrationCheck::class.java)

  @Singleton
  @Replaces(named = "jobsDatabaseMigrationCheck")
  @Named("jobsDatabaseMigrationCheck")
  fun jobsDatabaseMigrationCheck(): DatabaseMigrationCheck = Mockito.mock(DatabaseMigrationCheck::class.java)

  @Singleton
  @Replaces(DefaultJobPersistence::class)
  fun defaultJobPersistence(): DefaultJobPersistence = Mockito.mock(DefaultJobPersistence::class.java)

  @Singleton
  @Replaces(EventRunner::class)
  fun eventRunner(): EventRunner = Mockito.mock(EventRunner::class.java)

  @Singleton
  @Replaces(OAuthConfigSupplier::class)
  fun oauthConfigSupplier(): OAuthConfigSupplier = Mockito.mock(OAuthConfigSupplier::class.java)

  @Singleton
  @Replaces(TrackingClient::class)
  fun trackingClient(): TrackingClient = Mockito.mock(TrackingClient::class.java)

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
  fun mmDatabase(): Database = Mockito.mock(Database::class.java)

  private fun createMockDslContext(dataSource: DataSource): DSLContext {
    val configuration = Mockito.mock<Configuration>()
    val connectionProvider = Mockito.mock<DataSourceConnectionProvider>()
    val dslContext = Mockito.mock(DSLContext::class.java)
    Mockito.`when`(connectionProvider.dataSource()).thenReturn(dataSource)
    Mockito.`when`(configuration.connectionProvider()).thenReturn(connectionProvider)
    Mockito.`when`(dslContext.configuration()).thenReturn(configuration)
    return dslContext
  }
}
