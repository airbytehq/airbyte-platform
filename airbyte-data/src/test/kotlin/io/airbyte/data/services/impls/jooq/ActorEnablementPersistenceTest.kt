/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.domain.models.fusion.ActorEnablementFlags
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors

class ActorEnablementPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var ctx: DSLContext
  private lateinit var persistence: ActorEnablementPersistence
  private val organization = UUID.randomUUID()
  private val workspace = UUID.randomUUID()
  private val group = UUID.randomUUID()
  private val source = UUID.randomUUID()
  private val destination = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    ctx = database!!.query { it }
    persistence = ActorEnablementPersistence(database!!)
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
  }

  @Test
  fun `reads and mutations reject other tenants workspaces kinds and tombstones`() {
    assertEquals(ActorEnablementFlags(), persistence.read(organization, source, "source")!!.flags)
    assertNull(persistence.read(UUID.randomUUID(), source, "source"))
    assertNull(persistence.read(organization, destination, "source"))
    assertFalse(persistence.compareAndSet(UUID.randomUUID(), workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags(true, true)))
    assertFalse(
      persistence.compareAndSet(organization, UUID.randomUUID(), source, "source", ActorEnablementFlags(), ActorEnablementFlags(true, true)),
    )
    assertFalse(persistence.compareAndSet(organization, workspace, source, "destination", ActorEnablementFlags(), ActorEnablementFlags()))
    ctx.execute("UPDATE actor SET tombstone = true WHERE id = ?", source)
    assertNull(persistence.read(organization, source, "source"))
    assertFalse(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags()))
    ctx.execute("UPDATE workspace SET tombstone = true WHERE id = ?", workspace)
    assertNull(persistence.read(organization, destination, "destination"))
    assertFalse(
      persistence.compareAndSet(organization, workspace, destination, "destination", ActorEnablementFlags(), ActorEnablementFlags(true, true)),
    )
  }

  @Test
  fun `conditional updates compare complete state and clearing retains actor`() {
    assertTrue(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags(true, true)))
    assertFalse(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags()))
    assertTrue(
      persistence.compareAndSet(
        organization,
        workspace,
        source,
        "source",
        ActorEnablementFlags(true, true),
        ActorEnablementFlags(true),
      ),
    )
    assertEquals(ActorEnablementFlags(true), persistence.read(organization, source, "source")!!.flags)
    assertTrue(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(true), ActorEnablementFlags()))
    assertTrue(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags()))
    assertEquals(ActorEnablementFlags(), persistence.read(organization, source, "source")!!.flags)
  }

  @Test
  fun `destination CAS compares every flag and nullable start timestamp exactly`() {
    var current = ActorEnablementFlags()
    for (desired in listOf(
      ActorEnablementFlags(true),
      ActorEnablementFlags(true, true),
      ActorEnablementFlags(true, false, true),
      ActorEnablementFlags(true, false, true, Instant.EPOCH),
      ActorEnablementFlags(true, false, true, Instant.parse("0001-01-01T00:00:00Z")),
      ActorEnablementFlags(true, false, true, Instant.parse("9999-12-31T23:59:59.999999Z")),
      ActorEnablementFlags(true, false, true, Instant.parse("2026-09-17T12:34:56.123456Z")),
      ActorEnablementFlags(true, false, false, Instant.parse("2026-09-17T12:34:56.123456Z")),
      ActorEnablementFlags(true, false, false, null),
    )) {
      assertTrue(persistence.compareAndSet(organization, workspace, destination, "destination", current, desired))
      assertFalse(persistence.compareAndSet(organization, workspace, destination, "destination", current, current))
      assertEquals(desired, persistence.read(organization, destination, "destination")!!.flags)
      current = desired
    }
    assertTrue(persistence.compareAndSet(organization, workspace, destination, "destination", current, ActorEnablementFlags()))
    assertEquals(ActorEnablementFlags(), persistence.read(organization, destination, "destination")!!.flags)
  }

  @Test
  fun `equivalent timestamp offsets compare by instant and preserve microseconds`() {
    val flags = ActorEnablementFlags(true, false, true, Instant.parse("2026-09-17T12:34:56.123456Z"))
    assertTrue(persistence.compareAndSet(organization, workspace, destination, "destination", ActorEnablementFlags(), flags))
    val expected = flags.copy(backfillStartTime = OffsetDateTime.parse("2026-09-17T05:34:56.123456-07:00").toInstant())
    val disabled = expected.copy(enableAgentAccess = false, enableBackfill = false)
    assertTrue(persistence.compareAndSet(organization, workspace, destination, "destination", expected, disabled))
    assertEquals(disabled, persistence.read(organization, destination, "destination")!!.flags)
    assertFalse(persistence.compareAndSet(organization, workspace, destination, "destination", disabled.copy(backfillStartTime = null), flags))
    assertFalse(
      persistence.compareAndSet(
        organization,
        workspace,
        destination,
        "destination",
        disabled.copy(backfillStartTime = flags.backfillStartTime!!.plusNanos(1000)),
        flags,
      ),
    )
  }

  @Test
  fun `concurrent initializes have exactly one winner`() {
    val barrier = CyclicBarrier(2)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val results =
        pool
          .invokeAll(
            listOf(ActorEnablementFlags(true), ActorEnablementFlags(true, true)).map { value ->
              Callable {
                barrier.await()
                persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), value)
              }
            },
          ).map { it.get() }
      assertEquals(1, results.count { it })
      assertTrue(persistence.read(organization, source, "source")!!.flags.enableAgentAccess)
    } finally {
      pool.shutdownNow()
    }
  }

  @Test
  fun `sync read binds both actor tenants and service account to the assigned workload`() {
    val connection = UUID.randomUUID()
    val dataplane = UUID.randomUUID()
    val serviceAccount = UUID.randomUUID()
    val workload = "test-$connection"
    ctx.execute("INSERT INTO service_accounts (id, name, secret, managed) VALUES (?, 'test', 'unused-test-secret', true)", serviceAccount)
    ctx.execute(
      "INSERT INTO connection (id, namespace_definition, source_id, destination_id, name, catalog, status) VALUES (?, 'source', ?, ?, 'test', '{}'::jsonb, 'active')",
      connection,
      source,
      destination,
    )
    ctx.execute("INSERT INTO dataplane (id, dataplane_group_id, name, service_account_id) VALUES (?, ?, 'test', ?)", dataplane, group, serviceAccount)
    ctx.execute(
      "INSERT INTO workload (id, dataplane_id, status, input_payload, log_path, type, dataplane_group, workspace_id, organization_id) VALUES (?, ?, 'claimed', '{}', 'test', 'sync', ?, ?, ?)",
      workload,
      dataplane.toString(),
      group.toString(),
      workspace,
      organization,
    )
    assertTrue(persistence.compareAndSet(organization, workspace, source, "source", ActorEnablementFlags(), ActorEnablementFlags(true, true)))
    assertTrue(
      persistence.compareAndSet(organization, workspace, destination, "destination", ActorEnablementFlags(), ActorEnablementFlags(true, true)),
    )
    val result = persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, group, serviceAccount)!!
    assertEquals(true, result.sourceSearchIndexing)
    assertEquals(true, result.destinationSearchIndexing)
    assertNull(persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, group, UUID.randomUUID()))
    assertNull(persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, UUID.randomUUID(), serviceAccount))
    assertNull(persistence.assignedSyncInput(UUID.randomUUID(), workspace, connection, source, destination, workload, group, serviceAccount))
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
    assertNull(persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, group, serviceAccount))
    ctx.execute("UPDATE actor SET workspace_id = ? WHERE id = ?", workspace, destination)
    ctx.execute("UPDATE actor SET workspace_id = ? WHERE id = ?", otherWorkspace, source)
    assertNull(persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, group, serviceAccount))
    ctx.execute("UPDATE actor SET workspace_id = ? WHERE id = ?", workspace, source)
    ctx.execute("UPDATE workload SET organization_id = ? WHERE id = ?", UUID.randomUUID(), workload)
    assertNull(persistence.assignedSyncInput(organization, workspace, connection, source, destination, workload, group, serviceAccount))
  }

  @Test
  fun `ordinary jooq source and destination writers preserve enablements and new actors start empty`() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()
    val org = helper.organization!!.organizationId
    val ws = helper.workspace!!.workspaceId
    val sourceActor = helper.source!!.sourceId
    val destinationActor = helper.destination!!.destinationId
    assertTrue(persistence.compareAndSet(org, ws, sourceActor, "source", ActorEnablementFlags(), ActorEnablementFlags(true, true)))
    assertTrue(persistence.compareAndSet(org, ws, destinationActor, "destination", ActorEnablementFlags(), ActorEnablementFlags(true, true)))
    helper.createActorForActorDefinition(helper.sourceDefinition!!, sourceActor, ws, "edited source")
    helper.createActorForActorDefinition(helper.destinationDefinition!!, destinationActor, ws, "edited destination")
    assertEquals(ActorEnablementFlags(true, true), persistence.read(org, sourceActor, "source")!!.flags)
    assertEquals(ActorEnablementFlags(true, true), persistence.read(org, destinationActor, "destination")!!.flags)
    val newSource = helper.createActorForActorDefinition(helper.sourceDefinition!!)
    val newDestination = helper.createActorForActorDefinition(helper.destinationDefinition!!)
    assertEquals(ActorEnablementFlags(), persistence.read(org, newSource.sourceId, "source")!!.flags)
    assertEquals(ActorEnablementFlags(), persistence.read(org, newDestination.destinationId, "destination")!!.flags)
  }
}
