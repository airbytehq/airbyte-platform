/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.db.check.DatabaseCheckException;
import io.airbyte.db.check.JobsDatabaseAvailabilityCheck;
import org.jooq.DSLContext;
import org.jooq.Select;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link JobsDatabaseAvailabilityCheck} class.
 */
class JobsDatabaseAvailabilityCheckTest extends CommonDatabaseCheckTest {

  @Test
  void checkDatabaseAvailability() {
    final var check = new JobsDatabaseAvailabilityCheck(dslContext, TIMEOUT_MS);
    Assertions.assertDoesNotThrow(check::check);
  }

  @Test
  void checkDatabaseAvailabilityTimeout() {
    final DSLContext dslContext = mock(DSLContext.class);
    when(dslContext.fetchExists(any(Select.class))).thenThrow(new DataAccessException("test"));
    final var check = new JobsDatabaseAvailabilityCheck(dslContext, TIMEOUT_MS);
    Assertions.assertThrows(DatabaseCheckException.class, check::check);
  }

  @Test
  void checkDatabaseAvailabilityNullDslContext() {
    Assertions.assertThrows(NullPointerException.class, () -> new JobsDatabaseAvailabilityCheck(null, TIMEOUT_MS));
  }

}
