/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_41_009__AddBreakingChangeConfigOriginTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_41_009__AddBreakingChangeConfigOriginTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_41_009__AddBreakingChangeConfigOrigin()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testBreakingChangeOriginScopedConfig() {
    val ctx = dslContext!!

    insertConfigWithOriginType(
      ctx,
      V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType.BREAKING_CHANGE,
    ) // does not throw

    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) {
      insertConfigWithOriginType(
        ctx,
        V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType.valueOf(
          "foo",
        ),
      )
    }
  }

  companion object {
    private fun insertConfigWithOriginType(
      ctx: DSLContext,
      originType: V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType,
    ) {
      ctx
        .insertInto(DSL.table("scoped_configuration"))
        .columns(
          DSL.field("id"),
          DSL.field("key"),
          DSL.field("resource_type"),
          DSL.field("resource_id"),
          DSL.field("scope_type"),
          DSL.field("scope_id"),
          DSL.field("value"),
          DSL.field("origin_type"),
          DSL.field("origin"),
        ).values(
          UUID.randomUUID(),
          "some_key",
          V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION,
          UUID.randomUUID(),
          V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR,
          UUID.randomUUID(),
          "my_value",
          originType,
          "origin_ref",
        ).execute()
    }
  }
}
