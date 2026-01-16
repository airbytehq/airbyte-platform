/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
class V2_1_0_001__AddDataObservabilityStreamStatsAdditionalStatsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_8_1_006__AddStatusToSsoConfig",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDatabaseMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V1_8_1_006__AddStatusToSsoConfig()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDatabaseMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val context = dslContext!!
    assertFalse(additionalStatsColumExists(context))
    V2_1_0_001__AddDataObservabilityStreamStatsAdditionalStats.addAdditionalStatsColumn(context)
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
              .eq("observability_stream_stats")
              .and(DSL.field("column_name").eq("additional_stats")),
          ),
      )
  }
}
