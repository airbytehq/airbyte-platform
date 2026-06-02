/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
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
}
