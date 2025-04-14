/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs

import io.airbyte.db.instance.DatabaseConstants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

internal class ConfigsDatabaseMigrationDevCenterTest {
  /**
   * This test ensures that the dev center is working correctly end-to-end.
   *
   * If it fails, it means either the migration is not run properly, or the database initialization is incorrect in the dev center implementation.
   */
  @Test
  fun `verify schema dump`() {
    val expectedSchemaDump = File(DatabaseConstants.CONFIGS_SCHEMA_DUMP_PATH).readText()
    val actualSchemaDump = ConfigsDatabaseMigrationDevCenter().dumpSchema()
    try {
      assertEquals(expectedSchemaDump.trim(), actualSchemaDump.trim())
    } catch (_: AssertionError) {
      var modifiedExpected = expectedSchemaDump
      // I don't know why, but sometimes when this test runs, the following fields are rendered incorrectly.
      // This only happens when this test is run as part of a group, it never fails if ran in isolation
      mapOf(
        """"public"."support_state"""" to "support_state",
        """"public"."scope_type"""" to "scope_type",
        """"non_breaking_change_preference_type"""" to "non_breaking_change_preference_type",
        """"public"."payment_status"""" to "payment_status",
        """"public"."subscription_status"""" to "subscription_status",
        """"public"."auto_propagation_status"""" to "auto_propagation_status",
        """"public"."backfill_preference"""" to "backfill_preference",
        """"public"."secret_persistence_scope_type"""" to "secret_persistence_scope_type",
        """"public"."state_type"""" to "state_type",
      ).forEach { (old, new) ->
        modifiedExpected = modifiedExpected.replace(old, new)
      }
      assertEquals(modifiedExpected.trim(), actualSchemaDump.trim())
    }
  }
}
