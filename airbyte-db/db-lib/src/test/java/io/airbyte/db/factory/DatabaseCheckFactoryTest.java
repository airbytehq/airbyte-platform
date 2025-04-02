/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory;

import static org.mockito.Mockito.mock;

import io.airbyte.db.check.ConfigsDatabaseAvailabilityCheck;
import io.airbyte.db.check.ConfigsDatabaseMigrationCheck;
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck;
import io.airbyte.db.check.JobsDatabaseMigrationCheck;
import io.airbyte.db.init.ConfigsDatabaseInitializer;
import io.airbyte.db.init.JobsDatabaseInitializer;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link DatabaseCheckFactory} class.
 */
class DatabaseCheckFactoryTest {

  @Test
  void testCreateConfigsDatabaseAvailabilityCheck() {
    final var dslContext = mock(DSLContext.class);
    final var timeoutMs = 500L;
    final var check = DatabaseCheckFactory.createConfigsDatabaseAvailabilityCheck(dslContext, timeoutMs);

    Assertions.assertNotNull(check);
    Assertions.assertEquals(ConfigsDatabaseAvailabilityCheck.class, check.getClass());
    Assertions.assertEquals(timeoutMs, check.getTimeoutMs());
    Assertions.assertNotNull(check.getDslContext());
    Assertions.assertEquals(dslContext, check.getDslContext());
  }

  @Test
  void testCreateJobsDatabaseAvailabilityCheck() {
    final var dslContext = mock(DSLContext.class);
    final var timeoutMs = 500L;
    final var check = DatabaseCheckFactory.createJobsDatabaseAvailabilityCheck(dslContext, timeoutMs);

    Assertions.assertNotNull(check);
    Assertions.assertEquals(JobsDatabaseAvailabilityCheck.class, check.getClass());
    Assertions.assertEquals(timeoutMs, check.getTimeoutMs());
    Assertions.assertNotNull(check.getDslContext());
    Assertions.assertEquals(dslContext, check.getDslContext());
  }

  @Test
  void testCreateConfigsDatabaseMigrationCheck() {
    final var dslContext = mock(DSLContext.class);
    final var flyway = mock(Flyway.class);
    final var minimumMigrationVersion = "1.2.3";
    final var timeoutMs = 500L;
    final var check = DatabaseCheckFactory.createConfigsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs);

    Assertions.assertNotNull(check);
    Assertions.assertEquals(ConfigsDatabaseMigrationCheck.class, check.getClass());
    Assertions.assertNotNull(check.getDatabaseAvailabilityCheck());
    Assertions.assertEquals(ConfigsDatabaseAvailabilityCheck.class, check.getDatabaseAvailabilityCheck().getClass());
    Assertions.assertEquals(minimumMigrationVersion, check.getMinimumFlywayVersion());
    Assertions.assertEquals(timeoutMs, check.getTimeoutMs());
    Assertions.assertNotNull(check.getFlyway());
    Assertions.assertEquals(flyway, check.getFlyway());
  }

  @Test
  void testCreateJobsDatabaseMigrationCheck() {
    final var dslContext = mock(DSLContext.class);
    final var flyway = mock(Flyway.class);
    final var minimumMigrationVersion = "1.2.3";
    final var timeoutMs = 500L;
    final var check = DatabaseCheckFactory.createJobsDatabaseMigrationCheck(dslContext, flyway, minimumMigrationVersion, timeoutMs);

    Assertions.assertNotNull(check);
    Assertions.assertEquals(JobsDatabaseMigrationCheck.class, check.getClass());
    Assertions.assertNotNull(check.getDatabaseAvailabilityCheck());
    Assertions.assertEquals(JobsDatabaseAvailabilityCheck.class, check.getDatabaseAvailabilityCheck().getClass());
    Assertions.assertEquals(minimumMigrationVersion, check.getMinimumFlywayVersion());
    Assertions.assertEquals(timeoutMs, check.getTimeoutMs());
    Assertions.assertNotNull(check.getFlyway());
    Assertions.assertEquals(flyway, check.getFlyway());
  }

  @Test
  void testCreateConfigsDatabaseInitializer() {
    final var dslContext = mock(DSLContext.class);
    final var initialSchema = "SELECT 1;";
    final var timeoutMs = 500L;
    final var initializer = DatabaseCheckFactory.createConfigsDatabaseInitializer(dslContext, timeoutMs, initialSchema);

    Assertions.assertNotNull(initializer);
    Assertions.assertEquals(ConfigsDatabaseInitializer.class, initializer.getClass());
    Assertions.assertNotNull(initializer.getDatabaseAvailabilityCheck());
    Assertions.assertEquals(ConfigsDatabaseAvailabilityCheck.class, initializer.getDatabaseAvailabilityCheck().getClass());
    Assertions.assertEquals(timeoutMs, initializer.getDatabaseAvailabilityCheck().getTimeoutMs());
    Assertions.assertNotNull(initializer.getDslContext());
    Assertions.assertEquals(dslContext, initializer.getDslContext());
    Assertions.assertEquals(initialSchema, initializer.getInitialSchema());
  }

  @Test
  void testCreateJobsDatabaseInitializer() {
    final var dslContext = mock(DSLContext.class);
    final var initialSchema = "SELECT 1;";
    final var timeoutMs = 500L;
    final var initializer = DatabaseCheckFactory.createJobsDatabaseInitializer(dslContext, timeoutMs, initialSchema);

    Assertions.assertNotNull(initializer);
    Assertions.assertEquals(JobsDatabaseInitializer.class, initializer.getClass());
    Assertions.assertNotNull(initializer.getDatabaseAvailabilityCheck());
    Assertions.assertEquals(JobsDatabaseAvailabilityCheck.class, initializer.getDatabaseAvailabilityCheck().getClass());
    Assertions.assertEquals(timeoutMs, initializer.getDatabaseAvailabilityCheck().getTimeoutMs());
    Assertions.assertNotNull(initializer.getDslContext());
    Assertions.assertEquals(dslContext, initializer.getDslContext());
    Assertions.assertEquals(initialSchema, initializer.getInitialSchema());
  }

}
