/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_55_1_002__AddGenerationTable.Companion.createGenerationTable
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_55_1_002__AddGenerationTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_55_1_001__AddRefreshesTable",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_55_1_001__AddRefreshesTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val dslContext = getDslContext()
    val tableExists = generationTableExists(dslContext)

    Assertions.assertFalse(tableExists)

    createGenerationTable(dslContext)

    val tableExistsPostMigration = generationTableExists(dslContext)

    Assertions.assertTrue(tableExistsPostMigration)

    val index =
      dslContext
        .select()
        .from(DSL.table("pg_indexes"))
        .where(DSL.field("tablename").eq(V0_55_1_002__AddGenerationTable.STREAM_GENERATION_TABLE_NAME))
        .fetch()
        .map { c: Record -> c.getValue("indexdef", String::class.java) }
        .toSet()
    Assertions.assertEquals(3, index.size)
    Assertions.assertTrue(index.contains("CREATE UNIQUE INDEX stream_generation_pkey ON public.stream_generation USING btree (id)"))
    Assertions.assertTrue(
      index.contains(
        "CREATE INDEX stream_generation_connection_id_stream_name_generation_id_idx " +
          "ON public.stream_generation USING btree (connection_id, stream_name, generation_id DESC)",
      ),
    )
    Assertions.assertTrue(
      index.contains(
        "CREATE INDEX stream_generation_connection_id_stream_name_stream_namespac_idx ON public.stream_generation " +
          "USING btree (connection_id, stream_name, stream_namespace, generation_id DESC)",
      ),
    )
  }

  companion object {
    private fun generationTableExists(dslContext: DSLContext): Boolean {
      val size =
        dslContext
          .select()
          .from(DSL.table("pg_tables"))
          .where(DSL.field("tablename").eq(V0_55_1_002__AddGenerationTable.STREAM_GENERATION_TABLE_NAME))
          .fetch()
          .size
      return size > 0
    }
  }
}
