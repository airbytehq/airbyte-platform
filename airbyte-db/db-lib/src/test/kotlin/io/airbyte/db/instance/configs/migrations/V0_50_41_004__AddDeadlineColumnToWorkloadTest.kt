/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_41_004__AddDeadlineColumnToWorkload.Companion.addDeadlineColumnToWorkload
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_41_004__AddDeadlineColumnToWorkloadTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_41_004__AddDeadlineColumnToWorkloadTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_41_003__AddBackfillConfigToSchemaManagementTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val dslContext = getDslContext()
    val workloadIndexesBeforeMigration =
      dslContext
        .select()
        .from(DSL.table("pg_indexes"))
        .where(DSL.field("tablename").eq("workload"))
        .fetch()
        .map { c: Record -> c.getValue("indexname", String::class.java) }
        .toSet()
    Assertions.assertFalse(workloadIndexesBeforeMigration.contains("workload_deadline_idx"))

    addDeadlineColumnToWorkload(dslContext)

    val workloadIndexesAfterMigration =
      dslContext
        .select()
        .from(DSL.table("pg_indexes"))
        .where(DSL.field("tablename").eq("workload"))
        .fetch()
        .map { c: Record -> c.getValue("indexname", String::class.java) }
        .toSet()
    Assertions.assertTrue(workloadIndexesAfterMigration.contains("workload_deadline_idx"))
  }
}
