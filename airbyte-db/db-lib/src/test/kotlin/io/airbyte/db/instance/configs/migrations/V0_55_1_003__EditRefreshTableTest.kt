/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_55_1_003__EditRefreshTable.Companion.editRefreshTable
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
internal class V0_55_1_003__EditRefreshTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_55_1_002__AddGenerationTable",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_55_1_002__AddGenerationTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val dslContext = getDslContext()
    editRefreshTable(dslContext)
    val index =
      dslContext
        .select()
        .from(DSL.table("pg_indexes"))
        .where(DSL.field("tablename").eq(V0_55_1_003__EditRefreshTable.STREAM_REFRESHES_TABLE))
        .fetch()
        .map { c: Record -> c.getValue("indexdef", String::class.java) }
        .toSet()
    Assertions.assertEquals(4, index.size)
    Assertions.assertTrue(
      index.contains(
        "CREATE UNIQUE INDEX stream_refreshes_pkey ON public.stream_refreshes USING btree (id)",
      ),
    )
    Assertions.assertTrue(
      index.contains(
        "CREATE INDEX stream_refreshes_connection_id_idx ON public.stream_refreshes USING btree (connection_id)",
      ),
    )
    Assertions.assertTrue(
      index.contains(
        "CREATE INDEX stream_refreshes_connection_id_stream_name_idx ON public.stream_refreshes " +
          "USING btree (connection_id, stream_name)",
      ),
    )
    Assertions.assertTrue(
      index.contains(
        "CREATE INDEX stream_refreshes_connection_id_stream_name_stream_namespace_idx ON public.stream_refreshes" +
          " USING btree (connection_id, stream_name, stream_namespace)",
      ),
    )
  }
}
