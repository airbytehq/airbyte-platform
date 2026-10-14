/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_045__DropOAuthStateAndWorkspaceServiceAccountTablesTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `drops oauth_state and workspace_service_account tables`() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    DevDatabaseMigrator(
      ConfigsDatabaseMigrator(database!!, flyway),
      V2_1_0_044__CreateBillingNotificationSettingsTable().version,
    ).createBaseline()
    val ctx = dslContext!!
    ctx.execute("INSERT INTO oauth_state (id, state) VALUES ('temp_oauth_state.abc', '{}')")
    assertTrue(tableExists(ctx, "oauth_state"))
    assertTrue(tableExists(ctx, "workspace_service_account"))

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertFalse(tableExists(ctx, "oauth_state"))
    assertFalse(tableExists(ctx, "workspace_service_account"))
    assertTrue(tableExists(ctx, "dataplane_client_credentials"))
  }

  private fun tableExists(
    ctx: DSLContext,
    tableName: String,
  ): Boolean = ctx.fetchExists(ctx.select().from("information_schema.tables").where("table_schema = 'public' AND table_name = ?", tableName))
}
