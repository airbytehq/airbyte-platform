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
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_040__AddActorEnablementsTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `defaults existing and new actors and constrains enablements without changing actor lifecycle`() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    DevDatabaseMigrator(ConfigsDatabaseMigrator(database!!, flyway), V2_1_0_039__AddMaxTotalCpuRequestToDataWorkerUsage().version).createBaseline()
    val ctx = dslContext!!
    val workspace = UUID.randomUUID()
    val definition = UUID.randomUUID()
    val source = UUID.randomUUID()
    val organization = UUID.randomUUID()
    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'test', 'test@example.com')", organization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'test', ?, true, ?, (SELECT id FROM dataplane_group LIMIT 1))",
      workspace,
      workspace.toString(),
      organization,
    )
    ctx.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'test', 'source')", definition)
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'test', '{}'::jsonb, 'source')",
      source,
      workspace,
      definition,
    )
    ConfigsDatabaseMigrator(database!!, flyway).migrate()
    val constraints = listOf("actor_backfill_destination_only", "actor_backfill_requires_agent_access", "actor_indexing_requires_agent_access")
    assertEquals(
      constraints,
      ctx
        .fetch(
          "SELECT conname FROM pg_constraint WHERE contype = 'c' AND conrelid = 'actor'::regclass AND conname = ANY(?) ORDER BY conname",
          constraints.toTypedArray(),
        ).getValues("conname", String::class.java),
    )
    assertEquals(false, ctx.fetchValue("SELECT enable_agent_access OR enable_indexing OR enable_backfill FROM actor WHERE id = ?", source))
    ctx.execute("UPDATE actor SET enable_agent_access = true, enable_indexing = true WHERE id = ?", source)
    for (invalid in listOf(
      "enable_agent_access = NULL",
      "enable_indexing = NULL",
      "enable_backfill = NULL",
      "enable_agent_access = false",
      "enable_backfill = true",
      "backfill_start_time = '2026-09-17T12:34:56.123456Z'",
    )) {
      assertThrows(IntegrityConstraintViolationException::class.java) { ctx.execute("UPDATE actor SET $invalid WHERE id = ?", source) }
    }
    val destination = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'test', '{}'::jsonb, 'destination')",
      destination,
      workspace,
      definition,
    )
    assertEquals(false, ctx.fetchValue("SELECT enable_agent_access OR enable_indexing OR enable_backfill FROM actor WHERE id = ?", destination))
    assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) { ctx.execute("UPDATE actor SET enable_backfill = true WHERE id = ?", destination) }
    assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) { ctx.execute("UPDATE actor SET enable_indexing = true WHERE id = ?", destination) }
    ctx.execute(
      "UPDATE actor SET enable_agent_access = true, enable_backfill = true, backfill_start_time = '2026-09-17T12:34:56.123456Z' WHERE id = ?",
      destination,
    )
    assertEquals(
      Instant.parse("2026-09-17T12:34:56.123456Z"),
      ctx
        .fetchOne(
          "SELECT backfill_start_time FROM actor WHERE id = ?",
          destination,
        )!!
        .get("backfill_start_time", OffsetDateTime::class.java)
        .toInstant(),
    )
    ctx.execute("UPDATE actor SET enable_indexing = true, backfill_start_time = '2026-09-17T05:34:56.123456-07:00' WHERE id = ?", destination)
    assertEquals(
      Instant.parse("2026-09-17T12:34:56.123456Z"),
      ctx
        .fetchOne(
          "SELECT backfill_start_time FROM actor WHERE id = ?",
          destination,
        )!!
        .get("backfill_start_time", OffsetDateTime::class.java)
        .toInstant(),
    )
    ctx.execute("UPDATE actor SET name = 'edited', configuration = '{\"edited\":true}'::jsonb, tombstone = true WHERE id = ?", destination)
    assertEquals(true, ctx.fetchValue("SELECT enable_agent_access AND enable_indexing AND enable_backfill FROM actor WHERE id = ?", destination))
    ctx.execute("UPDATE actor SET enable_agent_access = false, enable_indexing = false WHERE id = ?", source)
    ctx.execute(
      "UPDATE actor SET enable_agent_access = false, enable_indexing = false, enable_backfill = false, backfill_start_time = NULL WHERE id = ?",
      destination,
    )
    ctx.execute("UPDATE actor SET backfill_start_time = '2026-09-17T12:34:56Z' WHERE id = ?", destination)
    assertEquals(false, ctx.fetchValue("SELECT enable_agent_access OR enable_backfill FROM actor WHERE id = ?", destination))
    ctx.execute("DELETE FROM actor WHERE id IN (?, ?)", source, destination)
    assertEquals(0, ctx.fetchValue("SELECT count(*)::int FROM actor WHERE id IN (?, ?)", source, destination))
  }
}
