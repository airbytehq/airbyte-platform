/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dsr

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

internal class DsrDeletionRequestTimeoutServiceTest {
  private lateinit var deletionRequestRepository: DataSubjectDeletionRequestRepository
  private val objectMapper = jacksonObjectMapper()
  private lateinit var service: DsrDeletionRequestTimeoutService

  private val requestId = UUID.randomUUID()
  private val userId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val email = "davin@example.com"
  private val datagrailId = "dg-123"

  @BeforeEach
  fun setup() {
    deletionRequestRepository = mockk()
    service = DsrDeletionRequestTimeoutService(deletionRequestRepository, objectMapper)
  }

  @Test
  fun `failTimedOutActiveRequest marks a stale running request failed and scrubs PII`() {
    val scrubbedManifestSlot = slot<String>()
    val confirmErrorsSlot = slot<String>()
    val executionCountsSlot = slot<String>()
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(runningRow())
    every {
      deletionRequestRepository.failRunningIfTimedOut(
        requestId = requestId,
        staleBefore = any(),
        completedAt = any(),
        scrubbedEmail = datagrailId,
        scrubbedManifest = capture(scrubbedManifestSlot),
        confirmErrors = capture(confirmErrorsSlot),
        executionCounts = capture(executionCountsSlot),
      )
    } returns 1

    val timedOut = service.failTimedOutActiveRequest(requestId, Duration.ofHours(2))

    assertTrue(timedOut)
    assertFalse(scrubbedManifestSlot.captured.contains(email))
    assertFalse(scrubbedManifestSlot.captured.contains("Davin"))
    assertFalse(scrubbedManifestSlot.captured.contains("postgres"))
    assertFalse(scrubbedManifestSlot.captured.contains("bigquery"))
    assertTrue(scrubbedManifestSlot.captured.contains(datagrailId))
    assertFalse(confirmErrorsSlot.captured.contains(email))
    assertTrue(confirmErrorsSlot.captured.contains("PT2H"))
    val executionCounts = objectMapper.readTree(executionCountsSlot.captured)
    assertEquals(0, executionCounts.get("deleted_jobs_count").asInt())
    assertEquals(0, executionCounts.get("deleted_keycloak_user_count").asInt())
  }

  @Test
  fun `failTimedOutActiveRequest leaves queued running requests unchanged`() {
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(runningRow())
    every {
      deletionRequestRepository.failRunningIfTimedOut(
        requestId = requestId,
        staleBefore = any(),
        completedAt = any(),
        scrubbedEmail = any(),
        scrubbedManifest = any(),
        confirmErrors = any(),
        executionCounts = any(),
      )
    } returns 0

    val timedOut = service.failTimedOutActiveRequest(requestId, Duration.ofHours(2))

    assertFalse(timedOut)
  }

  @Test
  fun `failTimedOutActiveRequest leaves non-running requests unchanged`() {
    every { deletionRequestRepository.findById(requestId) } returns
      Optional.of(runningRow().also { it.status = DataSubjectDeletionStatus.completed })

    val timedOut = service.failTimedOutActiveRequest(requestId, Duration.ofHours(2))

    assertFalse(timedOut)
    verify(exactly = 0) {
      deletionRequestRepository.failRunningIfTimedOut(any(), any(), any(), any(), any(), any(), any())
    }
  }

  @Test
  fun `recoverTimedOutRunningRequests fails stale active rows and resets stale queued rows`() {
    val activeFirst = runningRow(requestId = UUID.randomUUID())
    val activeSecond = runningRow(requestId = UUID.randomUUID())
    val queuedFirst = runningRow(requestId = UUID.randomUUID())
    val queuedSecond = runningRow(requestId = UUID.randomUUID())
    every { deletionRequestRepository.findRunningUpdatedBefore(any()) } returns listOf(activeFirst, activeSecond)
    every { deletionRequestRepository.findQueuedRunningUpdatedBefore(any()) } returns listOf(queuedFirst, queuedSecond)
    every {
      deletionRequestRepository.failRunningIfTimedOut(activeFirst.id!!, any(), any(), any(), any(), any(), any())
    } returns 1
    every {
      deletionRequestRepository.failRunningIfTimedOut(activeSecond.id!!, any(), any(), any(), any(), any(), any())
    } returns 0
    every {
      deletionRequestRepository.markPreviewedIfQueuedTimedOut(queuedFirst.id!!, any())
    } returns 1
    every {
      deletionRequestRepository.markPreviewedIfQueuedTimedOut(queuedSecond.id!!, any())
    } returns 0

    val recoveredCount = service.recoverTimedOutRunningRequests(Duration.ofHours(2))

    assertEquals(2, recoveredCount)
  }

  private fun runningRow(requestId: UUID = this.requestId): DataSubjectDeletionRequest =
    DataSubjectDeletionRequest(
      id = requestId,
      email = email,
      emailHash = "email-hash",
      datagrailId = datagrailId,
      status = DataSubjectDeletionStatus.running,
      userId = userId,
      requestedBy = "support@airbyte.io",
      oncallIssueNumber = "ONCALL-1234",
      confirmedBy = "reviewer@airbyte.io",
      confirmedAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(3),
      updatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(3),
      manifest = objectMapper.writeValueAsString(manifest()),
    )

  private fun manifest(): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin"),
      workspaceIds = listOf(workspaceId),
      workspaceRefs = listOf(DsrManifest.ManifestWorkspace(workspaceId, "Davin Workspace")),
      organizationIds = listOf(organizationId),
      organizationRefs = listOf(DsrManifest.ManifestOrganization(organizationId, "Davin Org")),
      connectionIds = listOf(connectionId),
      connectionRefs =
        listOf(
          DsrManifest.ManifestConnection(
            connectionId = connectionId,
            sourceId = sourceId,
            sourceName = "postgres",
            destinationId = destinationId,
            destinationName = "bigquery",
          ),
        ),
      sourceIds = listOf(sourceId),
      destinationIds = listOf(destinationId),
      connectorBuilderProjectIds = emptyList(),
      permissionIds = emptyList(),
      authUsers = emptyList(),
      jobCount = 7,
      attemptCount = 9,
      keycloakUsers = listOf(DsrManifest.ManifestKeycloakUser("auth-user-id", email, "Davin", true)),
      temporalWorkflows = emptyList(),
    )
}
