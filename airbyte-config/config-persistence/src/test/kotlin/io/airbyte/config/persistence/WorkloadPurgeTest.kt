/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import dev.failsafe.RetryPolicy
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPurgeBatchSize
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.micronaut.data.connection.exceptions.ConnectionException
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

internal class WorkloadPurgeTest : BaseConfigDatabaseTest() {
  private val now = OffsetDateTime.parse("2026-10-28T12:00:00Z")
  private lateinit var purge: WorkloadPurge
  private lateinit var transactions: TransactionOperations<Connection>
  private val featureFlags = mockk<FeatureFlagClient>()
  private val operations = mutableListOf<String>()
  private val batchDeletions = mutableListOf<Int>()
  private var failTransaction = 0
  private var transactionAttempts = 0
  private val connectionFailures = mutableMapOf<Int, RuntimeException>()
  private val transactionFailures = mutableMapOf<Int, RuntimeException>()
  private val commitAcknowledgementFailures = mutableMapOf<Int, RuntimeException>()
  private val transactionIds = mutableListOf<Long>()
  private val retryPolicy =
    RetryPolicy
      .builder(WorkloadPurgeRetryPolicy().workloadPurgeRetryPolicy().config)
      .withDelayFn { Duration.ZERO }
      .withMaxAttempts(3)
      .build()

  @BeforeEach
  fun setup() {
    truncateAllTables()
    transactionIds.clear()
    failTransaction = 0
    transactionAttempts = 0
    connectionFailures.clear()
    transactionFailures.clear()
    commitAcknowledgementFailures.clear()
    operations.clear()
    batchDeletions.clear()
    every { featureFlags.intVariation(WorkloadPurgeBatchSize, Empty) } answers {
      operations.add("flag")
      WorkloadPurgeBatchSize.default
    }
    transactions = mockk()
    every { transactions.executeWrite<Any>(any()) } answers {
      val callback = firstArg<TransactionCallback<Connection, Any>>()
      operations.add("transaction")
      transactionAttempts++
      connectionFailures.remove(transactionAttempts)?.let { throw it }
      if (failTransaction > 0 && transactionIds.size + 1 == failTransaction) {
        throw IllegalStateException("transaction unavailable")
      }
      val result =
        database!!.transaction { ctx ->
          transactionIds.add(ctx.fetchOne("SELECT txid_current()")!!.get(0, Long::class.java)!!)
          ctx.connectionResult { connection ->
            val status = mockk<TransactionStatus<Connection>>()
            every { status.connection } returns connection
            val deleted = callback.call(status)
            transactionFailures.remove(transactionAttempts)?.let { throw it }
            deleted
          }
        }
      commitAcknowledgementFailures.remove(transactionAttempts)?.let { throw it }
      batchDeletions.add(result as Int)
      result
    }
    purge = WorkloadPurge(transactions, featureFlags, retryPolicy = retryPolicy)
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
  fun `purge defaults to batches of 5000 with independent committed transactions`() {
    database!!.query { ctx ->
      (1..5001).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }

    assertEquals(5001, purge.prune(now))
    assertEquals(3, transactionIds.size, "$transactionIds")
    assertEquals(3, transactionIds.distinct().size, "$transactionIds")
  }

  @Test
  fun `temporary disconnection before transaction acquisition recovers`() {
    database!!.query { insertWorkload(it, "recover", WorkloadStatus.success, now.minusMonths(7)) }
    connectionFailures[1] = ConnectionException("connection unavailable", SQLException("lost connection", "08006"))

    assertEquals(1, purge.prune(now))
    assertEquals(3, transactionAttempts)
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
  }

  @Test
  fun `Micronaut closed connection cleanup failure without SQL state recovers`() {
    database!!.query { insertWorkload(it, "recover", WorkloadStatus.success, now.minusMonths(7)) }
    connectionFailures[1] =
      ConnectionException("Error setting JDBC Connection read-only: Connection is closed", SQLException("Connection is closed"))

    assertEquals(1, purge.prune(now))
    assertEquals(3, transactionAttempts)
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
  }

  @Test
  fun `batch size flag controls each committed batch`() {
    database!!.query { ctx ->
      (1..5).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }

    withBatchSizeFlag(2) { configured ->
      assertEquals(5, configured.prune(now))
      assertEquals(4, transactionIds.distinct().size, "$transactionIds")
    }
  }

  @Test
  fun `zero and negative batch size flags fail before opening a transaction`() {
    listOf(0, -1).forEach { batchSize ->
      assertThrows<IllegalArgumentException> { withBatchSizeFlag(batchSize) { it.prune(now) } }
    }
    assertEquals(0, transactionAttempts)
  }

  @Test
  fun `batch size flag is read again before every batch transaction`() {
    database!!.query { ctx ->
      (1..5).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }
    val sizes = ArrayDeque(listOf(2, 1, 3, 3))
    every { featureFlags.intVariation(WorkloadPurgeBatchSize, Empty) } answers {
      operations.add("flag")
      sizes.removeFirst()
    }

    assertEquals(5, purge.prune(now))

    assertEquals(listOf(2, 1, 2, 0), batchDeletions)
    assertEquals(listOf("flag", "transaction", "flag", "transaction", "flag", "transaction", "flag", "transaction"), operations)
  }

  @Test
  fun `batch size flag is read again before a retry transaction`() {
    database!!.query { ctx ->
      (1..3).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }
    connectionFailures[1] = ConnectionException("closed", SQLException("Connection is closed"))
    val sizes = ArrayDeque(listOf(1, 3, 3))
    every { featureFlags.intVariation(WorkloadPurgeBatchSize, Empty) } answers {
      operations.add("flag")
      sizes.removeFirst()
    }

    assertEquals(3, purge.prune(now))

    assertEquals(listOf(3, 0), batchDeletions)
    assertEquals(listOf("flag", "transaction", "flag", "transaction", "flag", "transaction"), operations)
  }

  @Test
  fun `later batch disconnect recovers without repeating earlier committed batches`() {
    database!!.query { ctx ->
      (1..3).forEach { insertWorkload(ctx, "recover-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }
    connectionFailures[2] = ConnectionException("restart", SQLException("shutdown", "57P01"))

    withBatchSizeFlag(2) { configured -> assertEquals(3, configured.prune(now)) }

    assertEquals(4, transactionAttempts)
    assertEquals(3, transactionIds.distinct().size, "$transactionIds")
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
  }

  @Test
  fun `disconnect inside a batch rolls back child deletion and retries in a fresh transaction`() {
    database!!.query { ctx ->
      (1..3).forEach {
        insertWorkload(ctx, "recover-$it", WorkloadStatus.success, now.minusMonths(7))
        insertQueue(ctx, "recover-$it")
      }
    }
    transactionFailures[1] = ConnectionException("lost connection", SQLException("disconnected", "08006"))

    withBatchSizeFlag(2) { configured -> assertEquals(3, configured.prune(now)) }

    assertEquals(4, transactionAttempts)
    assertEquals(4, transactionIds.distinct().size, "$transactionIds")
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD_QUEUE) })
  }

  @Test
  fun `uncertain commit retries remaining rows and counts only acknowledged deletions`() {
    database!!.query { ctx ->
      (1..3).forEach { insertWorkload(ctx, "recover-$it", WorkloadStatus.success, now.minusMonths(7)) }
    }
    commitAcknowledgementFailures[1] = ConnectionException("commit response lost", SQLException("disconnected", "08006"))

    withBatchSizeFlag(2) { configured -> assertEquals(1, configured.prune(now)) }

    assertEquals(3, transactionIds.distinct().size, "$transactionIds")
    assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
  }

  @Test
  fun `exhausted connection retries propagate without opening a transaction`() {
    val failure = ConnectionException("closed", SQLException("Connection is closed"))
    (1..3).forEach { connectionFailures[it] = failure }

    assertSame(failure, assertThrows<ConnectionException> { purge.prune(now) })

    assertEquals(3, transactionAttempts)
    assertEquals(emptyList<Long>(), transactionIds)
  }

  @Test
  fun `permanent SQL error propagates on the first attempt`() {
    val failure = ConnectionException("constraint", SQLException("foreign key", "23503"))
    connectionFailures[1] = failure

    assertSame(failure, assertThrows<ConnectionException> { purge.prune(now) })

    assertEquals(1, transactionAttempts)
    assertEquals(emptyList<Long>(), transactionIds)
  }

  @Test
  fun `later batch failure preserves already committed workload deletions`() {
    database!!.query { ctx ->
      (1..5001).forEach { insertWorkload(ctx, "batch-$it", WorkloadStatus.success, now.minusMonths(7)) }
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
  fun `exact deletion ignores the scheduled batch size flag`() {
    database!!.query { ctx ->
      (1..3).forEach { insertWorkload(ctx, "exact-$it", WorkloadStatus.running, now) }
    }

    withBatchSizeFlag(0) { configured ->
      assertEquals(3, configured.deleteByIds(listOf("exact-1", "exact-2", "exact-3")))
      assertEquals(1, transactionIds.size, "$transactionIds")
      assertEquals(0, database!!.query { it.fetchCount(Tables.WORKLOAD) })
    }
    verify(exactly = 0) { featureFlags.intVariation(WorkloadPurgeBatchSize, Empty) }
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

  private fun withBatchSizeFlag(
    batchSize: Int,
    action: (WorkloadPurge) -> Unit,
  ) {
    every { featureFlags.intVariation(WorkloadPurgeBatchSize, Empty) } answers {
      operations.add("flag")
      batchSize
    }
    action(purge)
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
