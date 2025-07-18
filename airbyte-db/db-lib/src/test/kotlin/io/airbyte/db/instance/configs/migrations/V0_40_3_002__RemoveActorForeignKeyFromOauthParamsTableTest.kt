/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_40_3_002__RemoveActorForeignKeyFromOauthParamsTable.Companion.removeActorDefinitionForeignKey
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
internal class V0_40_3_002__RemoveActorForeignKeyFromOauthParamsTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_40_3_002__RemoveActorForeignKeyFromOauthParamsTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_40_3_001__AddProtocolVersionToActorDefinition()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun test() {
    val context = dslContext!!
    Assertions.assertTrue(foreignKeyExists(context))
    removeActorDefinitionForeignKey(context)
    Assertions.assertFalse(foreignKeyExists(context))
  }

  companion object {
    protected fun foreignKeyExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.table_constraints")
          .where(
            DSL
              .field("table_name")
              .eq("actor_oauth_parameter")
              .and(DSL.field("constraint_name").eq("actor_oauth_parameter_actor_definition_id_fkey")),
          ),
      )
  }
}
