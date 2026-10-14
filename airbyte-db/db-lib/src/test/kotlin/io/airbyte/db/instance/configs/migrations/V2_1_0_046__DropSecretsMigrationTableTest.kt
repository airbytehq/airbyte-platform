/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.DSLContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_046__DropSecretsMigrationTableTest : AbstractConfigsDatabaseTest() {
  @AfterEach
  fun afterEach() {
    // The container is shared across tests, so reset the schema to re-run the migration.
    dslContext!!.dropSchemaIfExists("public").cascade().execute()
    dslContext!!.createSchema("public").execute()
    dslContext!!.setSchema("public").execute()
  }

  @Test
  fun `drops secrets_migration table when present`() {
    val flyway = migrateToBaseline()
    val ctx = dslContext!!
    ctx.execute("CREATE TABLE secrets_migration (organization_id uuid, workspace_id uuid, migrated boolean)")
    ctx.execute("INSERT INTO secrets_migration VALUES (gen_random_uuid(), gen_random_uuid(), false)")
    assertTrue(tableExists(ctx, "secrets_migration"))

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertFalse(tableExists(ctx, "secrets_migration"))
  }

  @Test
  fun `succeeds when secrets_migration table is absent`() {
    val flyway = migrateToBaseline()
    val ctx = dslContext!!
    assertFalse(tableExists(ctx, "secrets_migration"))

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertFalse(tableExists(ctx, "secrets_migration"))
  }

  private fun migrateToBaseline() =
    FlywayFactory
      .create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      ).also {
        DevDatabaseMigrator(
          ConfigsDatabaseMigrator(database!!, it),
          V2_1_0_045__DropOAuthStateAndWorkspaceServiceAccountTables().version,
        ).createBaseline()
      }

  private fun tableExists(
    ctx: DSLContext,
    tableName: String,
  ): Boolean = ctx.fetchExists(ctx.select().from("information_schema.tables").where("table_schema = 'public' AND table_name = ?", tableName))
}
