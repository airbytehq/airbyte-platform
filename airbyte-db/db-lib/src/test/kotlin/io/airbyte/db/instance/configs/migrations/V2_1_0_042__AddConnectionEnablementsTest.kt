/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.exception.IntegrityConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_042__AddConnectionEnablementsTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `adds non-null indexing enablement defaulting existing and new connections to disabled`() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    DevDatabaseMigrator(ConfigsDatabaseMigrator(database!!, flyway), V2_1_0_041__AddActorDraftState().version).createBaseline()
    val ctx = dslContext!!
    val organization = UUID.randomUUID()
    val workspace = UUID.randomUUID()
    val definition = UUID.randomUUID()
    val source = UUID.randomUUID()
    val destination = UUID.randomUUID()
    val existingConnection = UUID.randomUUID()

    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'test', 'test@example.com')", organization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'test', ?, true, ?, (SELECT id FROM dataplane_group LIMIT 1))",
      workspace,
      workspace.toString(),
      organization,
    )
    ctx.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'test', 'source')", definition)
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'source', '{}'::jsonb, 'source')",
      source,
      workspace,
      definition,
    )
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'destination', '{}'::jsonb, 'destination')",
      destination,
      workspace,
      definition,
    )
    ctx.execute(
      "INSERT INTO connection (id, namespace_definition, source_id, destination_id, name, catalog) " +
        "VALUES (?, 'source', ?, ?, 'existing', '{}'::jsonb)",
      existingConnection,
      source,
      destination,
    )

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertEquals(false, ctx.fetchValue("SELECT enable_indexing FROM connection WHERE id = ?", existingConnection))

    val newConnection = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO connection (id, namespace_definition, source_id, destination_id, name, catalog) " +
        "VALUES (?, 'source', ?, ?, 'new', '{}'::jsonb)",
      newConnection,
      source,
      destination,
    )
    assertEquals(false, ctx.fetchValue("SELECT enable_indexing FROM connection WHERE id = ?", newConnection))

    ctx.execute("UPDATE connection SET enable_indexing = true WHERE id = ?", newConnection)
    assertEquals(true, ctx.fetchValue("SELECT enable_indexing FROM connection WHERE id = ?", newConnection))

    assertThrows(IntegrityConstraintViolationException::class.java) {
      ctx.execute("UPDATE connection SET enable_indexing = NULL WHERE id = ?", newConnection)
    }
  }
}
