/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs

import io.airbyte.db.instance.DatabaseConstants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

internal class JobsDatabaseMigrationDevCenterTest {
  /**
   * This test ensures that the dev center is working correctly end-to-end. If it fails, it means
   * either the migration is not run properly, or the database initialization is incorrect.
   */
  @Test
  fun testSchemaDump() {
    val expectedSchmaDump = File(DatabaseConstants.JOBS_SCHEMA_DUMP_PATH).readText()
    val actualSchemaDump = JobsDatabaseMigrationDevCenter().dumpSchema()
    assertEquals(expectedSchmaDump.trim(), actualSchemaDump.trim())
  }
}
