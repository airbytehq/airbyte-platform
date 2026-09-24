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
internal class V2_1_0_041__AddActorDraftStateTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `adds non-null draft state defaulting existing and new actors to ready`() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    DevDatabaseMigrator(ConfigsDatabaseMigrator(database!!, flyway), V2_1_0_040__AddActorEnablements().version).createBaseline()
    val ctx = dslContext!!
    val organization = UUID.randomUUID()
    val workspace = UUID.randomUUID()
    val definition = UUID.randomUUID()
    val existingActor = UUID.randomUUID()

    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'test', 'test@example.com')", organization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'test', ?, true, ?, (SELECT id FROM dataplane_group LIMIT 1))",
      workspace,
      workspace.toString(),
      organization,
    )
    ctx.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'test', 'source')", definition)
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'existing', '{}'::jsonb, 'source')",
      existingActor,
      workspace,
      definition,
    )

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertEquals(false, ctx.fetchValue("SELECT is_draft FROM actor WHERE id = ?", existingActor))

    val defaultReadyActor = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'default ready', '{}'::jsonb, 'source')",
      defaultReadyActor,
      workspace,
      definition,
    )
    assertEquals(false, ctx.fetchValue("SELECT is_draft FROM actor WHERE id = ?", defaultReadyActor))

    val draftActor = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type, is_draft) VALUES (?, ?, ?, 'draft', '{}'::jsonb, 'source', true)",
      draftActor,
      workspace,
      definition,
    )
    assertEquals(true, ctx.fetchValue("SELECT is_draft FROM actor WHERE id = ?", draftActor))

    assertThrows(IntegrityConstraintViolationException::class.java) {
      ctx.execute("UPDATE actor SET is_draft = NULL WHERE id = ?", draftActor)
    }
  }
}
