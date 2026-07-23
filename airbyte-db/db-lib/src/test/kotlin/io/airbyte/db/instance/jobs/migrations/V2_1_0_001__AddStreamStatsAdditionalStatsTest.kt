/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.development.DevDatabaseMigrator
import io.airbyte.db.instance.jobs.AbstractJobsDatabaseTest
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
class V2_1_0_001__AddStreamStatsAdditionalStatsTest : AbstractJobsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_0_005__AdddScopeUpdatedAtIndex",
        JobsDatabaseMigrator.DB_IDENTIFIER,
        JobsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val jobsDatabaseMigrator = JobsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V1_1_0_005__AdddScopeUpdatedAtIndex()
    val devConfigsDbMigrator = DevDatabaseMigrator(jobsDatabaseMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val context = dslContext!!
    assertFalse(additionalStatsColumExists(context))
    V2_1_0_001__AddStreamStatsAdditionalStats.addAdditionalStatsColumn(context)
    assertTrue(additionalStatsColumExists(context))
  }

  companion object {
    private fun additionalStatsColumExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("stream_stats")
              .and(DSL.field("column_name").eq("additional_stats")),
          ),
      )
  }
}
