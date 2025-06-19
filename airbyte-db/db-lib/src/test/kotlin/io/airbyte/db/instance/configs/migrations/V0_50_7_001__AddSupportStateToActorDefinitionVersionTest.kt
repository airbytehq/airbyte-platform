/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_7_001__AddSupportStateToActorDefinitionVersion.Companion.addSupportStateColumnToActorDefinitionVersion
import io.airbyte.db.instance.configs.migrations.V0_50_7_001__AddSupportStateToActorDefinitionVersion.Companion.addSupportStateType
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
internal class V0_50_7_001__AddSupportStateToActorDefinitionVersionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_7_001__AddSupportStateTest.java",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_6_002__AddDefaultVersionIdToActor()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun test() {
    val context = getDslContext()
    addSupportStateType(context)
    addSupportStateColumnToActorDefinitionVersion(context)
    Assertions.assertTrue(typeExists(context, "support_state"))
    Assertions.assertTrue(columnExists(context, "support_state", "actor_definition_version"))

    // All support states should be set to "supported" after migration that added the column
    Assertions.assertEquals(
      context.fetchCount(
        DSL
          .select()
          .from("actor_definition_version"),
      ),
      context.fetchCount(
        DSL
          .select()
          .from("actor_definition_version")
          .where(
            DSL
              .field("support_state")
              .eq(V0_50_7_001__AddSupportStateToActorDefinitionVersion.SupportState.supported),
          ),
      ),
    )
  }

  companion object {
    fun typeExists(
      ctx: DSLContext,
      typeName: String?,
    ): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("pg_type")
          .where(DSL.field("typname").eq(typeName)),
      )

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
  }
}
