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
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_032__AddCommandsWorkloadIdIndexTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `creates commands workload id index`() {
    val ctx = baselineToPreviousMigration()

    assertNull(indexDefinition(ctx), "$INDEX_NAME should not exist before migration")

    V2_1_0_032__AddCommandsWorkloadIdIndex.addCommandsWorkloadIdIndex(ctx)

    assertEquals(
      "CREATE INDEX $INDEX_NAME ON public.$TABLE_NAME USING btree ($COLUMN_NAME)",
      indexDefinition(ctx),
    )
  }

  @Test
  fun `migration can run outside a transaction for concurrent index creation`() {
    assertFalse(V2_1_0_032__AddCommandsWorkloadIdIndex().canExecuteInTransaction())
  }

  private fun baselineToPreviousMigration(): DSLContext {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_032__AddCommandsWorkloadIdIndexTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_031__AddExecutionCountsToDataSubjectDeletionRequest()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    return dslContext!!
  }

  private fun indexDefinition(ctx: DSLContext): String? =
    ctx
      .fetchOne(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = ?
          AND indexname = ?
        """.trimIndent(),
        TABLE_NAME,
        INDEX_NAME,
      )?.get("indexdef", String::class.java)

  companion object {
    private const val TABLE_NAME = "commands"
    private const val COLUMN_NAME = "workload_id"
    private const val INDEX_NAME = "commands_workload_id_idx"
  }
}
