/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.check.DatabaseCheckException;
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck;
import io.airbyte.db.instance.DatabaseConstants;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link JobsDatabaseInitializer} class.
 */
class JobsDatabaseInitializerTest extends CommonDatabaseInitializerTest {

  @Test
  void testInitializingSchema() throws IOException {
    final var databaseAvailabilityCheck = mock(JobsDatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH);
    final var initializer = new JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);

    Assertions.assertDoesNotThrow(initializer::initialize);
    assertTrue(initializer.hasTable(dslContext, initializer.getTableNames().stream().findFirst().get()));
  }

  @Test
  void testInitializingSchemaAlreadyExists() throws IOException {
    final var databaseAvailabilityCheck = mock(JobsDatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH);
    dslContext.execute(initialSchema);
    final var initializer = new JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);

    Assertions.assertDoesNotThrow(initializer::initialize);
    assertTrue(initializer.hasTable(dslContext, initializer.getTableNames().stream().findFirst().get()));
  }

  @Test
  void testInitializationException() throws IOException, DatabaseCheckException {
    final var databaseAvailabilityCheck = mock(JobsDatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH);

    doThrow(new DatabaseCheckException("test")).when(databaseAvailabilityCheck).check();

    final var initializer = new JobsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);
    Assertions.assertThrows(DatabaseInitializationException.class, initializer::initialize);
  }

  @Test
  void testInitializationNullAvailabilityCheck() throws IOException {
    final var initialSchema = MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH);
    Assertions.assertThrows(NullPointerException.class, () -> new JobsDatabaseInitializer(null, dslContext, initialSchema));
  }

  @Test
  void testInitializationNullDslContext() throws IOException {
    final var databaseAvailabilityCheck = mock(JobsDatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.JOBS_INITIAL_SCHEMA_PATH);
    Assertions.assertThrows(NullPointerException.class, () -> new JobsDatabaseInitializer(databaseAvailabilityCheck, null, initialSchema));
  }

}
