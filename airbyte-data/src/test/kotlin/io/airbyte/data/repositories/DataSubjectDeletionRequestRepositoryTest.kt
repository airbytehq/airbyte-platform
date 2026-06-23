/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.db.instance.configs.jooq.generated.Tables.DATA_SUBJECT_DELETION_REQUEST
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest
internal class DataSubjectDeletionRequestRepositoryTest : AbstractConfigRepositoryTest() {
  private val repository = context.getBean(DataSubjectDeletionRequestRepository::class.java)!!

  @AfterEach
  fun cleanup() {
    repository.deleteAll()
  }

  @Test
  fun `cancelIfPreviewed cancels and scrubs only previewed requests`() {
    val requestId = UUID.randomUUID()
    repository.save(
      DataSubjectDeletionRequest(
        id = requestId,
        email = "davin@example.com",
        emailHash = "email-hash",
        datagrailId = "dg-123",
        status = DataSubjectDeletionStatus.previewed,
        userId = UUID.randomUUID(),
        requestedBy = "support@airbyte.io",
        oncallIssueNumber = "ONCALL-1234",
        manifest = """{"target_email":"davin@example.com"}""",
        prepareWarnings = """["contains davin@example.com"]""",
        confirmErrors = """["contains davin@example.com"]""",
      ),
    )

    val canceledAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    val updatedRows =
      repository.cancelIfPreviewed(
        requestId = requestId,
        canceledBy = "reviewer@airbyte.io",
        canceledAt = canceledAt,
        scrubbedEmail = "dg-123",
        scrubbedManifest = """{"target_email":"dg-123"}""",
      )

    assertEquals(1, updatedRows)
    val canceled = repository.findById(requestId).get()
    assertEquals(DataSubjectDeletionStatus.canceled, canceled.status)
    assertEquals("reviewer@airbyte.io", canceled.confirmedBy)
    assertEquals(canceledAt.toInstant(), canceled.confirmedAt!!.toInstant())
    assertEquals(canceledAt.toInstant(), canceled.completedAt!!.toInstant())
    assertEquals("dg-123", canceled.email)
    assertNull(canceled.prepareWarnings)
    assertNull(canceled.confirmErrors)
    assertTrue(canceled.manifest.contains("dg-123"))
    assertFalse(canceled.manifest.contains("davin@example.com"))
  }

  @Test
  fun `cancelIfPreviewed leaves running requests unchanged`() {
    val requestId = UUID.randomUUID()
    repository.save(
      DataSubjectDeletionRequest(
        id = requestId,
        email = "running@example.com",
        emailHash = "running-email-hash",
        datagrailId = "dg-running",
        status = DataSubjectDeletionStatus.running,
        userId = UUID.randomUUID(),
        requestedBy = "support@airbyte.io",
        oncallIssueNumber = "ONCALL-1234",
        confirmedBy = "executor@airbyte.io",
        manifest = """{"target_email":"running@example.com"}""",
      ),
    )

    val updatedRows =
      repository.cancelIfPreviewed(
        requestId = requestId,
        canceledBy = "reviewer@airbyte.io",
        canceledAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC),
        scrubbedEmail = "dg-running",
        scrubbedManifest = """{"target_email":"dg-running"}""",
      )

    assertEquals(0, updatedRows)
    val running = repository.findById(requestId).get()
    assertEquals(DataSubjectDeletionStatus.running, running.status)
    assertEquals("executor@airbyte.io", running.confirmedBy)
    assertEquals("running@example.com", running.email)
    assertTrue(running.manifest.contains("running@example.com"))
  }

  @Test
  fun `failRunningIfTimedOut fails only stale active running requests`() {
    val queuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val freshRequestId = UUID.randomUUID()
    repository.save(
      runningRequest(
        requestId = queuedRequestId,
        email = "queued@example.com",
        emailHash = "queued-email-hash",
        datagrailId = "dg-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    repository.save(
      runningRequest(
        requestId = freshRequestId,
        email = "fresh@example.com",
        emailHash = "fresh-email-hash",
        datagrailId = "dg-fresh",
      ),
    )
    val queuedTimestamp = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val activeConfirmedAt = OffsetDateTime.of(2026, 6, 2, 6, 0, 0, 0, ZoneOffset.UTC)
    val activeHeartbeatAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val now = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedTimestamp)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedTimestamp)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(queuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, activeConfirmedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, activeHeartbeatAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, now)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, now)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(freshRequestId))
      .execute()

    val staleRows = repository.findRunningUpdatedBefore(now.minusHours(2))

    assertEquals(listOf(activeRequestId), staleRows.map { it.id })
    val queuedUpdatedRows =
      repository.failRunningIfTimedOut(
        requestId = queuedRequestId,
        staleBefore = now.minusHours(2),
        completedAt = now,
        scrubbedEmail = "dg-queued",
        scrubbedManifest = """{"target_email":"dg-queued"}""",
        confirmErrors = """["Execution timed out after PT2H"]""",
        executionCounts = """{"deleted_jobs_count":0}""",
      )

    assertEquals(0, queuedUpdatedRows)
    val queued = repository.findById(queuedRequestId).get()
    assertEquals(DataSubjectDeletionStatus.running, queued.status)
    assertEquals("queued@example.com", queued.email)

    val updatedRows =
      repository.failRunningIfTimedOut(
        requestId = activeRequestId,
        staleBefore = now.minusHours(2),
        completedAt = now,
        scrubbedEmail = "dg-active",
        scrubbedManifest = """{"target_email":"dg-active"}""",
        confirmErrors = """["Execution timed out after PT2H"]""",
        executionCounts = """{"deleted_jobs_count":0}""",
      )

    assertEquals(1, updatedRows)
    val stale = repository.findById(activeRequestId).get()
    assertEquals(DataSubjectDeletionStatus.failed, stale.status)
    assertEquals("dg-active", stale.email)
    assertEquals(now.toInstant(), stale.completedAt!!.toInstant())
    assertNull(stale.prepareWarnings)
    assertTrue(stale.manifest.contains("dg-active"))
    assertTrue(stale.confirmErrors!!.contains("PT2H"))
    assertTrue(stale.executionCounts!!.contains("deleted_jobs_count"))

    val freshUpdatedRows =
      repository.failRunningIfTimedOut(
        requestId = freshRequestId,
        staleBefore = now.minusHours(2),
        completedAt = now,
        scrubbedEmail = "dg-fresh",
        scrubbedManifest = """{"target_email":"dg-fresh"}""",
        confirmErrors = """["Execution timed out after PT2H"]""",
        executionCounts = """{"deleted_jobs_count":0}""",
      )

    assertEquals(0, freshUpdatedRows)
    val fresh = repository.findById(freshRequestId).get()
    assertEquals(DataSubjectDeletionStatus.running, fresh.status)
    assertEquals("fresh@example.com", fresh.email)
    assertTrue(fresh.manifest.contains("fresh@example.com"))
  }

  @Test
  fun `markRunningExecutionStarted claims only queued running requests`() {
    val queuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val terminalRequestId = UUID.randomUUID()
    val queuedAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val startedAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = queuedRequestId,
        email = "queued@example.com",
        emailHash = "queued-email-hash",
        datagrailId = "dg-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    repository.save(
      runningRequest(
        requestId = terminalRequestId,
        email = "completed@example.com",
        emailHash = "completed-email-hash",
        datagrailId = "dg-completed",
      ).also { it.status = DataSubjectDeletionStatus.completed },
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(queuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt.minusHours(1))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()

    assertEquals(1, repository.markRunningExecutionStarted(queuedRequestId, startedAt))
    assertEquals(0, repository.markRunningExecutionStarted(activeRequestId, startedAt))
    assertEquals(0, repository.markRunningExecutionStarted(terminalRequestId, startedAt))
    assertEquals(
      startedAt.toInstant(),
      repository
        .findById(queuedRequestId)
        .get()
        .updatedAt!!
        .toInstant(),
    )
  }

  @Test
  fun `refreshQueuedRunningIfTimedOut requeues only stale queued running requests`() {
    val staleQueuedRequestId = UUID.randomUUID()
    val freshQueuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val oldQueuedAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val freshQueuedAt = OffsetDateTime.of(2026, 6, 2, 9, 30, 0, 0, ZoneOffset.UTC)
    val refreshedAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = staleQueuedRequestId,
        email = "stale-queued@example.com",
        emailHash = "stale-queued-email-hash",
        datagrailId = "dg-stale-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = freshQueuedRequestId,
        email = "fresh-queued@example.com",
        emailHash = "fresh-queued-email-hash",
        datagrailId = "dg-fresh-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, oldQueuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, oldQueuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(staleQueuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, freshQueuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, freshQueuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(freshQueuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, oldQueuedAt.minusHours(1))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, oldQueuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()

    assertEquals(
      1,
      repository.refreshQueuedRunningIfTimedOut(
        requestId = staleQueuedRequestId,
        email = "stale-queued@example.com",
        datagrailId = "dg-stale-queued",
        oncallIssueNumber = "ONCALL-1234",
        queuedBefore = refreshedAt.minusHours(2),
        refreshedAt = refreshedAt,
      ),
    )
    assertEquals(
      0,
      repository.refreshQueuedRunningIfTimedOut(
        requestId = freshQueuedRequestId,
        email = "fresh-queued@example.com",
        datagrailId = "dg-fresh-queued",
        oncallIssueNumber = "ONCALL-1234",
        queuedBefore = refreshedAt.minusHours(2),
        refreshedAt = refreshedAt,
      ),
    )
    assertEquals(
      0,
      repository.refreshQueuedRunningIfTimedOut(
        requestId = activeRequestId,
        email = "active@example.com",
        datagrailId = "dg-active",
        oncallIssueNumber = "ONCALL-1234",
        queuedBefore = refreshedAt.minusHours(2),
        refreshedAt = refreshedAt,
      ),
    )

    val staleQueued = repository.findById(staleQueuedRequestId).get()
    assertEquals(refreshedAt.toInstant(), staleQueued.confirmedAt!!.toInstant())
    assertEquals(refreshedAt.toInstant(), staleQueued.updatedAt!!.toInstant())
    assertEquals(
      freshQueuedAt.toInstant(),
      repository
        .findById(freshQueuedRequestId)
        .get()
        .updatedAt!!
        .toInstant(),
    )
    assertEquals(
      oldQueuedAt.toInstant(),
      repository
        .findById(activeRequestId)
        .get()
        .updatedAt!!
        .toInstant(),
    )
  }

  @Test
  fun `markPreviewedIfQueuedTimedOut resets only stale queued running requests`() {
    val staleQueuedRequestId = UUID.randomUUID()
    val freshQueuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val queuedAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val freshQueuedAt = OffsetDateTime.of(2026, 6, 2, 9, 30, 0, 0, ZoneOffset.UTC)
    val now = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = staleQueuedRequestId,
        email = "stale-queued@example.com",
        emailHash = "stale-queued-email-hash",
        datagrailId = "dg-stale-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = freshQueuedRequestId,
        email = "fresh-queued@example.com",
        emailHash = "fresh-queued-email-hash",
        datagrailId = "dg-fresh-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(staleQueuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, freshQueuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, freshQueuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(freshQueuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt.minusHours(1))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()

    val staleQueuedRows = repository.findQueuedRunningUpdatedBefore(now.minusHours(2))

    assertEquals(listOf(staleQueuedRequestId), staleQueuedRows.map { it.id })
    assertEquals(
      1,
      repository.markPreviewedIfQueuedTimedOut(
        requestId = staleQueuedRequestId,
        queuedBefore = now.minusHours(2),
      ),
    )
    assertEquals(
      0,
      repository.markPreviewedIfQueuedTimedOut(
        requestId = freshQueuedRequestId,
        queuedBefore = now.minusHours(2),
      ),
    )
    assertEquals(
      0,
      repository.markPreviewedIfQueuedTimedOut(
        requestId = activeRequestId,
        queuedBefore = now.minusHours(2),
      ),
    )

    val staleQueued = repository.findById(staleQueuedRequestId).get()
    assertEquals(DataSubjectDeletionStatus.previewed, staleQueued.status)
    assertNull(staleQueued.confirmedBy)
    assertNull(staleQueued.confirmedAt)

    assertEquals(DataSubjectDeletionStatus.running, repository.findById(freshQueuedRequestId).get().status)
    assertEquals(DataSubjectDeletionStatus.running, repository.findById(activeRequestId).get().status)
  }

  @Test
  fun `heartbeatRunningExecution refreshes only active running requests`() {
    val queuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val queuedAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val heartbeatAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = queuedRequestId,
        email = "queued@example.com",
        emailHash = "queued-email-hash",
        datagrailId = "dg-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(queuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt.minusHours(1))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()

    assertEquals(0, repository.heartbeatRunningExecution(queuedRequestId, heartbeatAt))
    assertEquals(1, repository.heartbeatRunningExecution(activeRequestId, heartbeatAt))
    assertEquals(
      queuedAt.toInstant(),
      repository
        .findById(queuedRequestId)
        .get()
        .updatedAt!!
        .toInstant(),
    )
    assertEquals(
      heartbeatAt.toInstant(),
      repository
        .findById(activeRequestId)
        .get()
        .updatedAt!!
        .toInstant(),
    )
  }

  @Test
  fun `finalizeRunningExecutionIfActive finalizes only active running requests`() {
    val queuedRequestId = UUID.randomUUID()
    val activeRequestId = UUID.randomUUID()
    val terminalRequestId = UUID.randomUUID()
    val queuedAt = OffsetDateTime.of(2026, 6, 2, 7, 0, 0, 0, ZoneOffset.UTC)
    val activeHeartbeatAt = OffsetDateTime.of(2026, 6, 2, 8, 0, 0, 0, ZoneOffset.UTC)
    val completedAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = queuedRequestId,
        email = "queued@example.com",
        emailHash = "queued-email-hash",
        datagrailId = "dg-queued",
      ),
    )
    repository.save(
      runningRequest(
        requestId = activeRequestId,
        email = "active@example.com",
        emailHash = "active-email-hash",
        datagrailId = "dg-active",
      ),
    )
    repository.save(
      runningRequest(
        requestId = terminalRequestId,
        email = "dg-terminal",
        emailHash = "terminal-email-hash",
        datagrailId = "dg-terminal",
      ).also {
        it.status = DataSubjectDeletionStatus.failed
        it.confirmErrors = """["Execution timed out after PT2H"]"""
        it.executionCounts = """{"deleted_jobs_count":0}"""
      },
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt)
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, queuedAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(queuedRequestId))
      .execute()
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, queuedAt.minusHours(1))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, activeHeartbeatAt)
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(activeRequestId))
      .execute()

    assertEquals(
      0,
      repository.finalizeRunningExecutionIfActive(
        requestId = queuedRequestId,
        finalStatus = DataSubjectDeletionStatus.completed,
        completedAt = completedAt,
        scrubbedEmail = "dg-queued",
        scrubbedManifest = """{"target_email":"dg-queued"}""",
        confirmErrors = null,
        executionCounts = """{"deleted_jobs_count":7}""",
      ),
    )
    assertEquals(
      1,
      repository.finalizeRunningExecutionIfActive(
        requestId = activeRequestId,
        finalStatus = DataSubjectDeletionStatus.completed,
        completedAt = completedAt,
        scrubbedEmail = "dg-active",
        scrubbedManifest = """{"target_email":"dg-active"}""",
        confirmErrors = null,
        executionCounts = """{"deleted_jobs_count":7}""",
      ),
    )
    assertEquals(
      0,
      repository.finalizeRunningExecutionIfActive(
        requestId = terminalRequestId,
        finalStatus = DataSubjectDeletionStatus.completed,
        completedAt = completedAt,
        scrubbedEmail = "dg-terminal-overwrite",
        scrubbedManifest = """{"target_email":"dg-terminal-overwrite"}""",
        confirmErrors = null,
        executionCounts = """{"deleted_jobs_count":7}""",
      ),
    )

    val queued = repository.findById(queuedRequestId).get()
    assertEquals(DataSubjectDeletionStatus.running, queued.status)
    assertEquals("queued@example.com", queued.email)

    val active = repository.findById(activeRequestId).get()
    assertEquals(DataSubjectDeletionStatus.completed, active.status)
    assertEquals("dg-active", active.email)
    assertEquals(completedAt.toInstant(), active.completedAt!!.toInstant())
    assertNull(active.prepareWarnings)
    assertNull(active.confirmErrors)
    assertTrue(active.manifest.contains("dg-active"))
    assertTrue(active.executionCounts!!.contains("deleted_jobs_count"))

    val terminal = repository.findById(terminalRequestId).get()
    assertEquals(DataSubjectDeletionStatus.failed, terminal.status)
    assertEquals("dg-terminal", terminal.email)
    assertTrue(terminal.confirmErrors!!.contains("PT2H"))
    assertTrue(terminal.executionCounts!!.contains("deleted_jobs_count"))
    assertTrue(terminal.executionCounts!!.contains("0"))
  }

  @Test
  fun `failRunningExecutionIfRunning cannot overwrite terminal requests`() {
    val runningRequestId = UUID.randomUUID()
    val terminalRequestId = UUID.randomUUID()
    val completedAt = OffsetDateTime.of(2026, 6, 2, 10, 0, 0, 0, ZoneOffset.UTC)
    repository.save(
      runningRequest(
        requestId = runningRequestId,
        email = "running@example.com",
        emailHash = "running-email-hash",
        datagrailId = "dg-running",
      ),
    )
    repository.save(
      runningRequest(
        requestId = terminalRequestId,
        email = "dg-terminal",
        emailHash = "terminal-email-hash",
        datagrailId = "dg-terminal",
      ).also {
        it.status = DataSubjectDeletionStatus.completed
        it.executionCounts = """{"deleted_jobs_count":7}"""
      },
    )
    jooqDslContext
      .update(DATA_SUBJECT_DELETION_REQUEST)
      .set(DATA_SUBJECT_DELETION_REQUEST.CONFIRMED_AT, completedAt.minusMinutes(5))
      .set(DATA_SUBJECT_DELETION_REQUEST.UPDATED_AT, completedAt.minusMinutes(5))
      .where(DATA_SUBJECT_DELETION_REQUEST.ID.eq(runningRequestId))
      .execute()

    assertEquals(
      1,
      repository.failRunningExecutionIfRunning(
        requestId = runningRequestId,
        completedAt = completedAt,
        scrubbedEmail = "dg-running",
        scrubbedManifest = """{"target_email":"dg-running"}""",
        confirmErrors = """["Background execution failed unexpectedly"]""",
        executionCounts = """{"deleted_jobs_count":0}""",
      ),
    )
    assertEquals(
      0,
      repository.failRunningExecutionIfRunning(
        requestId = terminalRequestId,
        completedAt = completedAt,
        scrubbedEmail = "dg-terminal-overwrite",
        scrubbedManifest = """{"target_email":"dg-terminal-overwrite"}""",
        confirmErrors = """["Background execution failed unexpectedly"]""",
        executionCounts = """{"deleted_jobs_count":0}""",
      ),
    )

    assertEquals(DataSubjectDeletionStatus.failed, repository.findById(runningRequestId).get().status)
    val terminal = repository.findById(terminalRequestId).get()
    assertEquals(DataSubjectDeletionStatus.completed, terminal.status)
    assertEquals("dg-terminal", terminal.email)
    assertTrue(terminal.executionCounts!!.contains("deleted_jobs_count"))
    assertTrue(terminal.executionCounts!!.contains("7"))
  }

  private fun runningRequest(
    requestId: UUID,
    email: String,
    emailHash: String,
    datagrailId: String,
  ): DataSubjectDeletionRequest =
    DataSubjectDeletionRequest(
      id = requestId,
      email = email,
      emailHash = emailHash,
      datagrailId = datagrailId,
      status = DataSubjectDeletionStatus.running,
      userId = UUID.randomUUID(),
      requestedBy = "support@airbyte.io",
      oncallIssueNumber = "ONCALL-1234",
      confirmedBy = "executor@airbyte.io",
      manifest = """{"target_email":"$email"}""",
    )
}
