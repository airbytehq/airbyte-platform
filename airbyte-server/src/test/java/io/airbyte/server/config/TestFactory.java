/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.db.Database;
import io.airbyte.db.check.DatabaseMigrationCheck;
import io.airbyte.persistence.job.DefaultJobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DataSourceConnectionProvider;
import org.mockito.Mockito;

/**
 * Test factory used to replace/override beans for unit tests.
 */
@Factory
public class TestFactory {

  @Singleton
  @Replaces(TemporalClient.class)
  public TemporalClient temporalClient() {
    return mock(TemporalClient.class);
  }

  @Singleton
  @Replaces(named = "configsDatabaseMigrationCheck")
  @Named("configsDatabaseMigrationCheck")
  public DatabaseMigrationCheck configsDatabaseMigrationCheck() {
    return mock(DatabaseMigrationCheck.class);
  }

  @Singleton
  @Replaces(named = "jobsDatabaseMigrationCheck")
  @Named("jobsDatabaseMigrationCheck")
  public DatabaseMigrationCheck jobsDatabaseMigrationCheck() {
    return mock(DatabaseMigrationCheck.class);
  }

  @Singleton
  @Replaces(DefaultJobPersistence.class)
  public DefaultJobPersistence defaultJobPersistence() {
    return mock(DefaultJobPersistence.class);
  }

  @Singleton
  @Replaces(EventRunner.class)
  public EventRunner eventRunner() {
    return mock(EventRunner.class);
  }

  @Singleton
  @Replaces(OAuthConfigSupplier.class)
  public OAuthConfigSupplier oauthConfigSupplier() {
    return mock(OAuthConfigSupplier.class);
  }

  @Singleton
  @Replaces(TrackingClient.class)
  public TrackingClient trackingClient() {
    return mock(TrackingClient.class);
  }

  @Singleton
  @Replaces(value = DSLContext.class,
            named = "config")
  @Named("config")
  public DSLContext configDslContext(@Named("config") final DataSource dataSource) {
    return createMockDslContext(dataSource);
  }

  @Singleton
  @Replaces(value = DSLContext.class,
            named = "jobs")
  @Named("jobs")
  public DSLContext jobsDslContext(@Named("jobs") final DataSource dataSource) {
    return createMockDslContext(dataSource);
  }

  @Singleton
  @Replaces(value = DSLContext.class,
            named = "local-secrets")
  @Named("local-secrets")
  public DSLContext localSecretsDslContext(@Named("local-secrets") final DataSource dataSource) {
    return createMockDslContext(dataSource);
  }

  @Singleton
  @Replaces(Database.class)
  @Named("configDatabase")
  public Database mmDatabase() {
    return Mockito.mock(Database.class);
  }

  private DSLContext createMockDslContext(final DataSource dataSource) {
    final Configuration configuration = mock();
    final DataSourceConnectionProvider connectionProvider = mock();
    final DSLContext dslContext = mock(DSLContext.class);
    when(connectionProvider.dataSource()).thenReturn(dataSource);
    when(configuration.connectionProvider()).thenReturn(connectionProvider);
    when(dslContext.configuration()).thenReturn(configuration);
    return dslContext;
  }

}
