/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import static org.mockito.Mockito.mock;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.db.check.DatabaseMigrationCheck;
import io.airbyte.persistence.job.DefaultJobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

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

}
