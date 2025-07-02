/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_40_18_002__AddActorDefinitionNormalizationAndDbtColumns.Companion.addNormalizationRepositoryColumn
import io.airbyte.db.instance.configs.migrations.V0_40_18_002__AddActorDefinitionNormalizationAndDbtColumns.Companion.addNormalizationTagColumn
import io.airbyte.db.instance.configs.migrations.V0_40_18_002__AddActorDefinitionNormalizationAndDbtColumns.Companion.addSupportsDbtColumn
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_40_18_002__AddActorDefinitionNormalizationAndDbtColumnsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_40_18_001__AddInvalidProtocolFlagToConnections",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_40_18_001__AddInvalidProtocolFlagToConnections()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(Exception::class)
  fun test() {
    val context = dslContext!!
    Assertions.assertFalse(columnExists(context, "normalization_repository"))
    Assertions.assertFalse(columnExists(context, "normalization_tag"))
    Assertions.assertFalse(columnExists(context, "supports_dbt"))
    addNormalizationRepositoryColumn(context)
    Assertions.assertTrue(columnExists(context, "normalization_repository"))
    addNormalizationTagColumn(context)
    Assertions.assertTrue(columnExists(context, "normalization_tag"))
    addSupportsDbtColumn(context)
    Assertions.assertTrue(columnExists(context, "supports_dbt"))
  }

  companion object {
    fun columnExists(
      ctx: DSLContext,
      columnName: String?,
    ): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq(columnName)),
          ),
      )
  }
}
