/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_6_002__AddDefaultVersionIdToActor.Companion.addDefaultVersionIdColumnToActor
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_6_002__AddDefaultVersionIdToActorTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_6_002__AddDefaultVersionIdToActorTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_6_001__DropUnsupportedProtocolFlagCol()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun test() {
    val context = getDslContext()
    addDefaultVersionIdColumnToActor(context)
    Assertions.assertTrue(columnExists(context, "default_version_id", "actor"))
    Assertions.assertTrue(foreignKeyExists(context, "default_version_id", "actor"))
  }

  companion object {
    fun columnExists(
      ctx: DSLContext,
      columnName: String?,
      tableName: String?,
    ): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq(tableName)
              .and(DSL.field("column_name").eq(columnName)),
          ),
      )

    protected fun foreignKeyExists(
      ctx: DSLContext,
      columnName: String,
      tableName: String,
    ): Boolean {
      val constraintName = tableName + "_" + columnName + "_fkey"
      return ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.table_constraints")
          .where(
            DSL
              .field("table_name")
              .eq(tableName)
              .and(DSL.field("constraint_name").eq(constraintName)),
          ),
      )
    }
  }
}
