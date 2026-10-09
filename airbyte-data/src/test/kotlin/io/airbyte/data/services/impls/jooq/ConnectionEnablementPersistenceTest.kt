/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.domain.models.fusion.ConnectionEnablementFlags
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors

class ConnectionEnablementPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var ctx: DSLContext
  private lateinit var persistence: ConnectionEnablementPersistence
  private val organization = UUID.randomUUID()
  private val workspace = UUID.randomUUID()
  private val group = UUID.randomUUID()
  private val source = UUID.randomUUID()
  private val destination = UUID.randomUUID()
  private val connection = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    ctx = database!!.query { it }
    persistence = ConnectionEnablementPersistence(database!!)
    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'test', 'test@example.com')", organization)
    ctx.execute("INSERT INTO dataplane_group (id, organization_id, name, enabled, tombstone) VALUES (?, ?, 'test', true, false)", group, organization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'test', ?, true, ?, ?)",
      workspace,
      workspace.toString(),
      organization,
      group,
    )
    for ((actor, type) in listOf(source to "source", destination to "destination")) {
      val definition = UUID.randomUUID()
      ctx.execute("INSERT INTO actor_definition (id, name, actor_type) VALUES (?, 'test', ?::actor_type)", definition, type)
      ctx.execute(
        "INSERT INTO actor (id, workspace_id, actor_definition_id, name, configuration, actor_type) VALUES (?, ?, ?, 'test', '{}'::jsonb, ?::actor_type)",
        actor,
        workspace,
        definition,
        type,
      )
    }
    ctx.execute(
      "INSERT INTO connection (id, namespace_definition, source_id, destination_id, name, catalog, status) " +
        "VALUES (?, 'source', ?, ?, 'test', '{}'::jsonb, 'active')",
      connection,
      source,
      destination,
    )
  }

  @Test
  fun `reads and mutations reject other tenants deprecated connections and tombstones`() {
    assertEquals(ConnectionEnablementFlags(), persistence.read(organization, connection)!!.flags)
    assertNull(persistence.read(UUID.randomUUID(), connection))
    assertNull(persistence.read(organization, UUID.randomUUID()))
    assertFalse(
      persistence.compareAndSet(UUID.randomUUID(), workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)),
    )
    assertFalse(
      persistence.compareAndSet(organization, UUID.randomUUID(), connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)),
    )
    ctx.execute("UPDATE connection SET status = 'deprecated' WHERE id = ?", connection)
    assertNull(persistence.read(organization, connection))
    assertFalse(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)))
    ctx.execute("UPDATE connection SET status = 'active' WHERE id = ?", connection)
    ctx.execute("UPDATE actor SET tombstone = true WHERE id = ?", source)
    assertNull(persistence.read(organization, connection))
    ctx.execute("UPDATE actor SET tombstone = false WHERE id = ?", source)
    ctx.execute("UPDATE workspace SET tombstone = true WHERE id = ?", workspace)
    assertNull(persistence.read(organization, connection))
    assertFalse(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)))
  }

  @Test
  fun `read denies a connection whose actors sit in different organizations`() {
    val otherOrganization = UUID.randomUUID()
    val otherWorkspace = UUID.randomUUID()
    ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'other', 'other@example.com')", otherOrganization)
    ctx.execute(
      "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'other', ?, true, ?, ?)",
      otherWorkspace,
      otherWorkspace.toString(),
      otherOrganization,
      group,
    )
    ctx.execute("UPDATE actor SET workspace_id = ? WHERE id = ?", otherWorkspace, destination)
    assertNull(persistence.read(organization, connection))
    assertNull(persistence.read(otherOrganization, connection))
    assertFalse(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)))
  }

  @Test
  fun `conditional updates compare complete state and clearing retains the connection`() {
    assertTrue(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(true)))
    assertFalse(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags()))
    assertEquals(ConnectionEnablementFlags(true), persistence.read(organization, connection)!!.flags)
    assertTrue(persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(true), ConnectionEnablementFlags()))
    assertEquals(ConnectionEnablementFlags(), persistence.read(organization, connection)!!.flags)
  }

  @Test
  fun `concurrent initializes have exactly one winner`() {
    val barrier = CyclicBarrier(2)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val results =
        pool
          .invokeAll(
            listOf(true, true).map { value ->
              Callable {
                barrier.await()
                persistence.compareAndSet(organization, workspace, connection, ConnectionEnablementFlags(), ConnectionEnablementFlags(value))
              }
            },
          ).map { it.get() }
      assertEquals(1, results.count { it })
      assertTrue(persistence.read(organization, connection)!!.flags.enableIndexing)
    } finally {
      pool.shutdownNow()
    }
  }
}
