/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.Select
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

/**
 * Test suite for the [JobsDatabaseAvailabilityCheck] class.
 */
internal class JobsDatabaseAvailabilityCheckTest : CommonsDatabaseCheckTest() {
  @Test
  fun checkDatabaseAvailability() {
    val check = JobsDatabaseAvailabilityCheck(dslContext, TIMEOUT_MS)
    assertDoesNotThrow { check.check() }
  }

  @Test
  fun checkDatabaseAvailabilityTimeout() {
    val dslContext =
      mockk<DSLContext> {
        every { fetchExists(any<Select<org.jooq.Record>>()) } throws DataAccessException("test")
      }

    val check = JobsDatabaseAvailabilityCheck(dslContext, TIMEOUT_MS)
    assertThrows<DatabaseCheckException> { check.check() }
  }
}
