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
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_006__AddLabelsJsonbColumnToWorkloadTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_006__AddLabelsJsonbColumnToWorkloadTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_005__AddLockedStatusAndStatusReason()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
  }

  @Test
  fun testMigration() {
    // Run the migration
    V2_1_0_006__AddLabelsJsonbColumnToWorkload.addLabelsColumn(ctx)

    // Verify the labels column exists
    val fields =
      ctx
        .meta()
        .getTables("workload")
        .first()
        .fields()
    val labelsField = fields.find { it.name == "labels" }
    Assertions.assertNotNull(labelsField, "labels column should exist")
    Assertions.assertEquals(SQLDataType.JSONB, labelsField?.dataType, "labels column should be JSONB type")

    // Test inserting a workload with JSONB labels
    val workloadId = "test-workload-${UUID.randomUUID()}"
    val labelsJson = """{"job_id": "123", "attempt_id": "1", "connection_id": "conn-456"}"""

    ctx
      .insertInto(DSL.table("workload"))
      .columns(
        DSL.field("id"),
        DSL.field("status"),
        DSL.field("input_payload"),
        DSL.field("log_path"),
        DSL.field("type"),
        DSL.field("labels"),
      ).values(
        workloadId,
        DSL.cast(DSL.inline("pending"), SQLDataType.VARCHAR.asEnumDataType(V0_50_33_001__AddWorkloadTable.WorkloadStatus::class.java)),
        "test-payload",
        "/logs/test",
        DSL.cast(DSL.inline("sync"), SQLDataType.VARCHAR.asEnumDataType(V0_50_33_008__AddMutexKeyAndTypeColumnsToWorkload.WorkloadType::class.java)),
        JSONB.jsonb(labelsJson),
      ).execute()

    // Verify we can query the JSONB column
    val result =
      ctx
        .select(DSL.field("id"), DSL.field("labels"))
        .from(DSL.table("workload"))
        .where(DSL.field("id").eq(workloadId))
        .fetchOne()

    Assertions.assertNotNull(result)
    Assertions.assertEquals(workloadId, result?.get("id"))

    // Verify JSONB data is retrievable
    val labels = result?.get("labels")
    Assertions.assertNotNull(labels)

    // Test querying with JSONB operators (containment)
    val countWithJobId =
      ctx
        .selectCount()
        .from(DSL.table("workload"))
        .where(
          DSL.condition("labels @> ?::jsonb", """{"job_id": "123"}"""),
        ).fetchOne(0, Int::class.java)

    Assertions.assertEquals(1, countWithJobId, "Should find workload with job_id label")

    // Test that workloads without labels column (NULL) still work
    val workloadId2 = "test-workload-null-${UUID.randomUUID()}"
    ctx
      .insertInto(DSL.table("workload"))
      .columns(
        DSL.field("id"),
        DSL.field("status"),
        DSL.field("input_payload"),
        DSL.field("log_path"),
        DSL.field("type"),
      ).values(
        workloadId2,
        DSL.cast(DSL.inline("pending"), SQLDataType.VARCHAR.asEnumDataType(V0_50_33_001__AddWorkloadTable.WorkloadStatus::class.java)),
        "test-payload-2",
        "/logs/test-2",
        DSL.cast(DSL.inline("sync"), SQLDataType.VARCHAR.asEnumDataType(V0_50_33_008__AddMutexKeyAndTypeColumnsToWorkload.WorkloadType::class.java)),
      ).execute()

    val result2 =
      ctx
        .select(DSL.field("id"), DSL.field("labels"))
        .from(DSL.table("workload"))
        .where(DSL.field("id").eq(workloadId2))
        .fetchOne()

    Assertions.assertNotNull(result2)
    Assertions.assertNull(result2?.get("labels"), "labels should be null")
  }

  @Test
  fun testMigrationIsIdempotent() {
    // Run migration twice to ensure it's idempotent
    V2_1_0_006__AddLabelsJsonbColumnToWorkload.addLabelsColumn(ctx)

    // Run again - should not fail
    V2_1_0_006__AddLabelsJsonbColumnToWorkload.addLabelsColumn(ctx)

    // Verify column still exists and is correct type
    val fields =
      ctx
        .meta()
        .getTables("workload")
        .first()
        .fields()
    val labelsField = fields.find { it.name == "labels" }
    Assertions.assertNotNull(labelsField)
    Assertions.assertEquals(SQLDataType.JSONB, labelsField?.dataType)
  }
}
