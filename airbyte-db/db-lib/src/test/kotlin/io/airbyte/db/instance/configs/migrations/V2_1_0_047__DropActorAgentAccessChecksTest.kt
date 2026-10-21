/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_047__DropActorAgentAccessChecksTest : AbstractConfigsDatabaseTest() {
  @AfterEach
  fun afterEach() {
    // The container is shared across tests, so reset the schema to re-run the migration.
    dslContext!!.dropSchemaIfExists("public").cascade().execute()
    dslContext!!.createSchema("public").execute()
    dslContext!!.setSchema("public").execute()
  }

  @Test
  fun `revoking agent access no longer requires clearing legacy indexing and backfill`() {
    val flyway = migrateToBaseline()
    val ctx = dslContext!!
    val destination = insertDestination()
    ctx.execute("UPDATE actor SET enable_agent_access = true, enable_indexing = true, enable_backfill = true WHERE id = ?", destination)
    assertThrows<DataAccessException> { ctx.execute("UPDATE actor SET enable_agent_access = false WHERE id = ?", destination) }

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    ctx.execute("UPDATE actor SET enable_agent_access = false WHERE id = ?", destination)
    assertEquals(emptyList<String>(), agentAccessChecks())
  }

  @Test
  fun `succeeds when the checks are already absent`() {
    val flyway = migrateToBaseline()
    val ctx = dslContext!!
    ctx.execute("ALTER TABLE actor DROP CONSTRAINT actor_indexing_requires_agent_access, DROP CONSTRAINT actor_backfill_requires_agent_access")

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertEquals(emptyList<String>(), agentAccessChecks())
  }

  private fun agentAccessChecks(): List<String> =
    dslContext!!
      .fetch(
        "SELECT conname FROM pg_constraint WHERE conrelid = 'actor'::regclass AND conname IN (?, ?)",
        "actor_indexing_requires_agent_access",
        "actor_backfill_requires_agent_access",
      ).getValues("conname", String::class.java)

  private fun insertDestination(): UUID {
    val ctx = dslContext!!
    val organization = UUID.randomUUID()
    val workspace = UUID.randomUUID()
    val definition = UUID.randomUUID()
    val destination = UUID.randomUUID()
    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'test', 'test@example.com')", organization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'test', ?, true, ?, (SELECT id FROM dataplane_group LIMIT 1))",
      workspace,
      workspace.toString(),
      organization,
    )
    ctx.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'test', 'destination')", definition)
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'test', '{}'::jsonb, 'destination')",
      destination,
      workspace,
      definition,
    )
    return destination
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
          V2_1_0_046__DropSecretsMigrationTable().version,
        ).createBaseline()
      }
}
