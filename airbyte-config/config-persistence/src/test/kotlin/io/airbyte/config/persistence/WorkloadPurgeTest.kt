/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.UUID

internal class WorkloadPurgeTest : BaseConfigDatabaseTest() {
  private val now = OffsetDateTime.parse("2026-10-28T12:00:00Z")
  private lateinit var purge: WorkloadPurge
  private var failTransaction = 0
  private val transactionIds = mutableListOf<Long>()

  @BeforeEach
  fun setup() {
    truncateAllTables()
    transactionIds.clear()
    failTransaction = 0
    val transactions = mockk<TransactionOperations<Connection>>()
    every { transactions.executeWrite<Any>(any()) } answers {
      val callback = firstArg<TransactionCallback<Connection, Any>>()
      if (failTransaction > 0 && transactionIds.size + 1 == failTransaction) {
        throw IllegalStateException("transaction unavailable")
      }
      database!!.transaction { ctx ->
        transactionIds.add(ctx.fetchOne("SELECT txid_current()")!!.get(0, Long::class.java)!!)
        ctx.connectionResult { connection ->
          val status = mockk<TransactionStatus<Connection>>()
          every { status.connection } returns connection
          callback.call(status)
        }
      }
    }
    purge = WorkloadPurge(transactions)
  }

  @Test
  fun `empty purge deletes nothing`() {
    assertEquals(0, purge.prune(now))
  }

  @Test
  fun `purge deletes only terminal workloads older than six months since last update`() {
    database!!.query { ctx ->
      WorkloadStatus.entries.forEach { status -> insertWorkload(ctx, "old-$status", status, now.minusMonths(7)) }
      insertWorkload(ctx, "recent", WorkloadStatus.success, now.minusMonths(1))
      insertWorkload(ctx, "boundary", WorkloadStatus.success, now.minusMonths(6))
      ctx.execute("UPDATE workload SET created_at = ?::timestamptz WHERE id = 'recent'", now.minusYears(1))
    }

    assertEquals(3, purge.prune(now))
    val remaining =
      database!!.query {
        it
          .select(Tables.WORKLOAD.ID)
          .from(Tables.WORKLOAD)
          .fetch(Tables.WORKLOAD.ID)
          .toSet()
      }
    assertEquals(
      WorkloadStatus.entries
        .filterNot { it in setOf(WorkloadStatus.success, WorkloadStatus.failure, WorkloadStatus.cancelled) }
        .map { "old-$it" }
        .toSet() + setOf("recent", "boundary"),
      remaining,
    )
  }

  @Test
  fun `purge uses batches of 500 with independent committed transactions`() {
    database!!.query { ctx ->
      (1..501).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }

    assertEquals(501, purge.prune(now))
    assertEquals(3, transactionIds.size)
    assertEquals(3, transactionIds.distinct().size)
  }

  @Test
  fun `later batch failure preserves already committed workload deletions`() {
    database!!.query { ctx ->
      (1..501).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }
    failTransaction = 2

    assertThrows<IllegalStateException> { purge.prune(now) }

    assertEquals(1, database!!.query { it.fetchCount(Tables.WORKLOAD) })
    assertEquals(1, transactionIds.size)
  }

  @Test
  fun `exact deletion removes queue rows and ignores other workload ids`() {
    database!!.query { ctx ->
      insertWorkload(ctx, "target", WorkloadStatus.running, now)
      insertWorkload(ctx, "target-neighbor", WorkloadStatus.running, now)
      insertQueue(ctx, "target")
      insertQueue(ctx, "target-neighbor")
    }

    assertEquals(1, purge.deleteByIds(listOf("target", "target", "missing")))
    assertEquals(listOf("target-neighbor"), database!!.query { it.select(Tables.WORKLOAD.ID).from(Tables.WORKLOAD).fetch(Tables.WORKLOAD.ID) })
    assertEquals(
      listOf("target-neighbor"),
      database!!.query {
        it.select(Tables.WORKLOAD_QUEUE.WORKLOAD_ID).from(Tables.WORKLOAD_QUEUE).fetch(Tables.WORKLOAD_QUEUE.WORKLOAD_ID)
      },
    )
  }

  @Test
  fun `scheduled purge deletes queued workloads and cascades their commands`() {
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    database!!.query { ctx ->
      ctx.execute("INSERT INTO organization (id, name, email) VALUES (?, 'org', 'purge@example.com')", organizationId)
      ctx.execute("INSERT INTO dataplane_group (id, organization_id, name) VALUES (?, ?, 'group')", dataplaneGroupId, organizationId)
      ctx.execute(
        "INSERT INTO workspace (id, name, slug, initial_setup_complete, organization_id, dataplane_group_id) VALUES (?, 'ws', 'ws', true, ?, ?)",
        workspaceId,
        organizationId,
        dataplaneGroupId,
      )
      insertWorkload(ctx, "command-workload", WorkloadStatus.success, now.minusMonths(7))
      insertQueue(ctx, "command-workload")
      ctx.execute(
        "INSERT INTO commands (id, workload_id, command_type, command_input, workspace_id, organization_id) VALUES ('command', 'command-workload', 'replicate', '{}', ?, ?)",
        workspaceId,
        organizationId,
      )
    }

    assertEquals(1, purge.prune(now))

    assertEquals(0, database!!.query { it.fetchCount(Tables.COMMANDS) })
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD_QUEUE) })
    assertEquals(1, database!!.query { it.fetchCount(Tables.WORKSPACE) })
  }

  @Test
  fun `empty exact deletion does not open a transaction`() {
    assertEquals(0, purge.deleteByIds(emptyList()))
    assertEquals(emptyList<Long>(), transactionIds)
  }

  @Test
  fun `failed workload deletion rolls back queue deletion and propagates error`() {
    database!!.query { ctx ->
      insertWorkload(ctx, "blocked", WorkloadStatus.success, now.minusMonths(7))
      insertQueue(ctx, "blocked")
      ctx.execute("CREATE TABLE purge_blocker (workload_id varchar REFERENCES workload(id))")
      ctx.execute("INSERT INTO purge_blocker VALUES ('blocked')")
    }
    try {
      assertThrows<RuntimeException> { purge.prune(now) }
      assertEquals(1, database!!.query { it.fetchCount(Tables.WORKLOAD) })
      assertEquals(1, database!!.query { it.fetchCount(Tables.WORKLOAD_QUEUE) })
    } finally {
      database!!.query { it.execute("DROP TABLE purge_blocker") }
    }
  }

  private fun insertWorkload(
    ctx: DSLContext,
    id: String,
    status: WorkloadStatus,
    updatedAt: OffsetDateTime,
  ) {
    ctx.execute(
      "INSERT INTO workload (id, status, input_payload, log_path, type, updated_at) VALUES (?, ?::workload_status, '{}', 'log', 'sync', ?::timestamptz)",
      id,
      status.literal,
      updatedAt,
    )
  }

  private fun insertQueue(
    ctx: DSLContext,
    workloadId: String,
  ) {
    ctx.execute("INSERT INTO workload_queue (id, dataplane_group, priority, workload_id) VALUES (?, 'group', 0, ?)", UUID.randomUUID(), workloadId)
  }
}
