/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_048__AddConnectWorkloadTypeTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_048__AddConnectWorkloadTypeTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_047__DropActorAgentAccessChecks()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
  }

  /**
   * Postgres cannot remove an enum label, and the test container is shared across the methods of a
   * test class, so the pre-migration state is only observable once. Everything this migration
   * guarantees is therefore asserted in a single method.
   */
  @Test
  fun `adds connect to the workload type enum`() {
    val before = enumValues(WORKLOAD_TYPE)
    assertFalse(before.contains(CONNECT))

    V2_1_0_048__AddConnectWorkloadType.runMigration(ctx)

    val after = enumValues(WORKLOAD_TYPE)
    assertTrue(after.contains(CONNECT))
    assertTrue(after.containsAll(before))
    assertEquals(before.size + 1, after.size)

    // `ADD VALUE IF NOT EXISTS` makes a repeat run a no-op.
    V2_1_0_048__AddConnectWorkloadType.runMigration(ctx)
    assertEquals(after, enumValues(WORKLOAD_TYPE))
  }

  private fun enumValues(typeName: String): List<String> =
    ctx
      .fetch(
        """
        SELECT enum_label.enumlabel
        FROM pg_type type_metadata
        JOIN pg_enum enum_label ON enum_label.enumtypid = type_metadata.oid
        WHERE type_metadata.typname = ?
        ORDER BY enum_label.enumsortorder
        """.trimIndent(),
        typeName,
      ).map { it.get("enumlabel") as String }

  companion object {
    private const val WORKLOAD_TYPE = "workload_type"
    private const val CONNECT = "connect"
  }
}
