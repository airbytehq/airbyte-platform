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
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_038__CreateDataWorkerAllocatedCapacityTableTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_038__CreateDataWorkerAllocatedCapacityTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_037__AllowActorScopedEditorInvitations()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
    ctx.execute("DROP TABLE IF EXISTS $TABLE_NAME")
  }

  @Test
  fun `creates the allocated capacity table`() {
    assertFalse(tableExists(), "$TABLE_NAME should not exist before migration")

    V2_1_0_038__CreateDataWorkerAllocatedCapacityTable.createDataWorkerAllocatedCapacityTable(ctx)

    assertTrue(tableExists())
    assertColumn("id", "uuid", isNullable = false)
    assertColumn("organization_id", "uuid", isNullable = false)
    assertColumn("dataplane_group_id", "uuid", isNullable = false)
    assertColumn("allocated_capacity", "real", isNullable = false, defaultContains = "0")
    assertColumn("created_at", "timestamp with time zone", isNullable = false)
    assertColumn("updated_at", "timestamp with time zone", isNullable = false)

    assertEquals(
      "CREATE UNIQUE INDEX $UNIQUE_INDEX_NAME ON public.$TABLE_NAME " +
        "USING btree (organization_id, dataplane_group_id)",
      indexDefinition(UNIQUE_INDEX_NAME),
    )
  }

  @Test
  fun `allows one fractional allocation per organization and region`() {
    V2_1_0_038__CreateDataWorkerAllocatedCapacityTable.createDataWorkerAllocatedCapacityTable(ctx)

    val organizationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()

    insertAllocation(organizationId, dataplaneGroupId, 1.5)

    assertEquals(
      1.5f,
      ctx.fetchValue(
        "SELECT allocated_capacity FROM $TABLE_NAME WHERE organization_id = ? AND dataplane_group_id = ?",
        organizationId,
        dataplaneGroupId,
      ),
    )

    // A second row for the same (organization, region) pair violates the unique index.
    assertThrows(DataAccessException::class.java) {
      insertAllocation(organizationId, dataplaneGroupId, 2.0)
    }

    // The same region in a different organization, and a different region in the same
    // organization, are both independent allocations.
    insertAllocation(UUID.randomUUID(), dataplaneGroupId, 2.0)
    insertAllocation(organizationId, UUID.randomUUID(), 2.0)
    assertEquals(3, ctx.fetchValue("SELECT COUNT(*)::int FROM $TABLE_NAME"))
  }

  @Test
  fun `rejects negative allocated capacity`() {
    V2_1_0_038__CreateDataWorkerAllocatedCapacityTable.createDataWorkerAllocatedCapacityTable(ctx)

    assertThrows(DataAccessException::class.java) {
      insertAllocation(UUID.randomUUID(), UUID.randomUUID(), -1.0)
    }

    // Zero is a meaningful allocation: it is how a region is explicitly given no capacity.
    insertAllocation(UUID.randomUUID(), UUID.randomUUID(), 0.0)
  }

  @Test
  fun `defaults allocated capacity to zero`() {
    V2_1_0_038__CreateDataWorkerAllocatedCapacityTable.createDataWorkerAllocatedCapacityTable(ctx)

    val organizationId = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO $TABLE_NAME (id, organization_id, dataplane_group_id) VALUES (?, ?, ?)",
      UUID.randomUUID(),
      organizationId,
      UUID.randomUUID(),
    )

    assertEquals(
      0.0f,
      ctx.fetchValue("SELECT allocated_capacity FROM $TABLE_NAME WHERE organization_id = ?", organizationId),
    )
  }

  private fun insertAllocation(
    organizationId: UUID,
    dataplaneGroupId: UUID,
    allocatedCapacity: Double,
  ) {
    ctx.execute(
      """
      INSERT INTO $TABLE_NAME (id, organization_id, dataplane_group_id, allocated_capacity)
      VALUES (?, ?, ?, ?)
      """.trimIndent(),
      UUID.randomUUID(),
      organizationId,
      dataplaneGroupId,
      allocatedCapacity,
    )
  }

  private fun tableExists(): Boolean =
    ctx.fetchExists(
      ctx
        .selectOne()
        .from("information_schema.tables")
        .where("table_schema = 'public'")
        .and("table_name = ?", TABLE_NAME),
    )

  private fun assertColumn(
    columnName: String,
    expectedDataType: String,
    isNullable: Boolean,
    defaultContains: String? = null,
  ) {
    val column =
      ctx.fetchOne(
        """
        SELECT data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
        """.trimIndent(),
        TABLE_NAME,
        columnName,
      ) ?: error("Column $columnName does not exist on $TABLE_NAME")

    assertEquals(expectedDataType, column.get("data_type", String::class.java), "$columnName data type")
    assertEquals(
      if (isNullable) "YES" else "NO",
      column.get("is_nullable", String::class.java),
      "$columnName nullability",
    )
    if (defaultContains != null) {
      val columnDefault = column.get("column_default", String::class.java)
      assertTrue(
        columnDefault != null && columnDefault.contains(defaultContains),
        "$columnName default <$columnDefault> should contain <$defaultContains>",
      )
    }
  }

  private fun indexDefinition(indexName: String): String? =
    ctx
      .fetchOne(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = ? AND indexname = ?
        """.trimIndent(),
        TABLE_NAME,
        indexName,
      )?.get("indexdef", String::class.java)

  companion object {
    private const val TABLE_NAME = "data_worker_allocated_capacity"
    private const val UNIQUE_INDEX_NAME = "data_worker_allocated_capacity_org_dataplane_group_idx"
  }
}
