/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.server.handlers.dsr.DsrDeletionService
import io.airbyte.commons.server.handlers.dsr.DsrInvalidConfirmationException
import io.airbyte.commons.server.handlers.dsr.DsrInvalidStateException
import io.airbyte.commons.server.handlers.dsr.DsrRequestNotFoundException
import io.airbyte.commons.server.handlers.dsr.DsrUserNotFoundException
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.airbyte.domain.services.dsr.DsrManifest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import java.util.concurrent.Executor

internal class DsrDeletionControllerTest {
  private lateinit var dsrDeletionService: DsrDeletionService
  private lateinit var controller: DsrDeletionController
  private val objectMapper = jacksonObjectMapper()

  private val email = "davin+2@airbyte.io"
  private val datagrailId = "dg-abc-123"
  private val oncallIssueNumber = "ONCALL-1234"
  private val requestedBy = "support@airbyte.io"
  private val executedBy = "reviewer@airbyte.io"
  private val requestId: UUID = UUID.randomUUID()
  private val userId: UUID = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    dsrDeletionService = mockk(relaxed = false)
    controller = DsrDeletionController(dsrDeletionService, objectMapper, Executor { it.run() })
  }

  @Test
  fun `preview delegates to service and maps the response fields`() {
    every {
      dsrDeletionService.preview(email, datagrailId, oncallIssueNumber, requestedBy)
    } returns
      DsrDeletionService.PreviewResult(
        requestId = requestId,
        status = "PREVIEWED",
        manifest = manifest(),
        warnings = listOf("a warning"),
      )

    val resp =
      controller.preview(
        DsrPreviewRequest(
          email = email,
          datagrailId = datagrailId,
          oncallIssueNumber = oncallIssueNumber,
          requestedBy = requestedBy,
        ),
      )

    assertNotNull(resp)
    assertEquals("PREVIEWED", resp!!.status)
    assertEquals(requestId, resp.requestId)
    assertEquals(listOf("a warning"), resp.warnings)
    assertEquals(email, resp.manifest.targetEmail)
    verify(exactly = 1) { dsrDeletionService.preview(email, datagrailId, oncallIssueNumber, requestedBy) }
  }

  @Test
  fun `execute claims the request, enqueues deletion, and returns accepted`() {
    every {
      dsrDeletionService.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    } returns
      DsrDeletionService.StartExecutionResult(
        requestId = requestId,
        status = "RUNNING",
        started = true,
      )
    every { dsrDeletionService.executeClaimedRequest(requestId) } returns
      DsrDeletionService.ExecuteResult(
        requestId = requestId,
        status = "COMPLETED",
        deletedJobsCount = 17,
        deletedAttemptsCount = 34,
        deletedConnectionsCount = 2,
        deletedActorsCount = 4,
        deletedBuilderProjectsCount = 1,
        deletedPermissionsCount = 5,
        deletedWorkspacesCount = 1,
        deletedOrganizationsCount = 1,
        deletedAuthUsersCount = 2,
        tombstonedUser = true,
        deletedKeycloakUserCount = 1,
        terminatedTemporalWorkflowCount = 2,
        errors = emptyList(),
      )

    val resp =
      controller.execute(
        requestId,
        DsrExecuteRequest(
          email = email,
          datagrailId = datagrailId,
          oncallIssueNumber = oncallIssueNumber,
          executedBy = executedBy,
        ),
      )

    assertNotNull(resp)
    assertEquals(202, resp!!.status.code)
    val body = resp.body()
    assertNotNull(body)
    assertEquals(requestId, body.requestId)
    assertEquals("RUNNING", body.status)
    assertEquals(true, body.started)
    assertEquals("/api/v1/internal/dsr/$requestId", body.statusUrl)
    assertEquals("DSR deletion request accepted and started. Poll status_url for completion.", body.message)
    verify(exactly = 1) { dsrDeletionService.executeClaimedRequest(requestId) }
  }

  @Test
  fun `execute marks the request failed when background deletion throws unexpectedly`() {
    val failure = IllegalStateException("background failure")
    every {
      dsrDeletionService.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    } returns
      DsrDeletionService.StartExecutionResult(
        requestId = requestId,
        status = "RUNNING",
        started = true,
      )
    every { dsrDeletionService.executeClaimedRequest(requestId) } throws failure
    every { dsrDeletionService.failRunningRequestUnexpectedly(requestId, any<Throwable>()) } returns
      DsrDeletionService.ExecuteResult(
        requestId = requestId,
        status = "FAILED",
        deletedJobsCount = 0,
        deletedAttemptsCount = 0,
        deletedConnectionsCount = 0,
        deletedActorsCount = 0,
        deletedBuilderProjectsCount = 0,
        deletedPermissionsCount = 0,
        deletedWorkspacesCount = 0,
        deletedOrganizationsCount = 0,
        deletedAuthUsersCount = 0,
        tombstonedUser = false,
        deletedKeycloakUserCount = 0,
        terminatedTemporalWorkflowCount = 0,
        errors = listOf("Background execution failed unexpectedly: background failure"),
      )

    val resp =
      controller.execute(
        requestId,
        DsrExecuteRequest(
          email = email,
          datagrailId = datagrailId,
          oncallIssueNumber = oncallIssueNumber,
          executedBy = executedBy,
        ),
      )

    assertNotNull(resp)
    assertEquals(202, resp!!.status.code)
    verify(exactly = 1) { dsrDeletionService.executeClaimedRequest(requestId) }
    verify(exactly = 1) { dsrDeletionService.failRunningRequestUnexpectedly(requestId, failure) }
  }

  @Test
  fun `execute does not mark the request failed when duplicate worker sees an active execution`() {
    val failure = DsrInvalidStateException("Deletion request $requestId is already being executed by another worker.")
    every {
      dsrDeletionService.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    } returns
      DsrDeletionService.StartExecutionResult(
        requestId = requestId,
        status = "RUNNING",
        started = true,
      )
    every { dsrDeletionService.executeClaimedRequest(requestId) } throws failure

    val resp =
      controller.execute(
        requestId,
        DsrExecuteRequest(
          email = email,
          datagrailId = datagrailId,
          oncallIssueNumber = oncallIssueNumber,
          executedBy = executedBy,
        ),
      )

    assertNotNull(resp)
    assertEquals(202, resp!!.status.code)
    verify(exactly = 1) { dsrDeletionService.executeClaimedRequest(requestId) }
    verify(exactly = 0) { dsrDeletionService.failRunningRequestUnexpectedly(any(), any()) }
  }

  @Test
  fun `execute returns accepted without enqueueing when request is already running`() {
    every {
      dsrDeletionService.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    } returns
      DsrDeletionService.StartExecutionResult(
        requestId = requestId,
        status = "RUNNING",
        started = false,
      )

    val resp =
      controller.execute(
        requestId,
        DsrExecuteRequest(
          email = email,
          datagrailId = datagrailId,
          oncallIssueNumber = oncallIssueNumber,
          executedBy = executedBy,
        ),
      )

    assertNotNull(resp)
    assertEquals(202, resp!!.status.code)
    val body = resp.body()
    assertNotNull(body)
    assertEquals("RUNNING", body.status)
    assertEquals(false, body.started)
    assertEquals("/api/v1/internal/dsr/$requestId", body.statusUrl)
    assertEquals("DSR deletion request is already running. Poll status_url for completion.", body.message)
    verify(exactly = 0) { dsrDeletionService.executeClaimedRequest(any()) }
  }

  @Test
  fun `DSR write endpoints audit only the actor and not request or response bodies`() {
    assertEquals(
      AuditLoggingProvider.ONLY_ACTOR,
      DsrDeletionController::class.java
        .getMethod("preview", DsrPreviewRequest::class.java)
        .getAnnotation(AuditLogging::class.java)
        .provider,
    )
    assertEquals(
      AuditLoggingProvider.ONLY_ACTOR,
      DsrDeletionController::class.java
        .getMethod("execute", UUID::class.java, DsrExecuteRequest::class.java)
        .getAnnotation(AuditLogging::class.java)
        .provider,
    )
    assertEquals(
      AuditLoggingProvider.ONLY_ACTOR,
      DsrDeletionController::class.java
        .getMethod("cancel", UUID::class.java, DsrCancelRequest::class.java)
        .getAnnotation(AuditLogging::class.java)
        .provider,
    )
  }

  @Test
  fun `getById returns 404 when the request is not found`() {
    every { dsrDeletionService.get(requestId) } returns null

    val resp = controller.getById(requestId)
    assertEquals(404, resp.status.code)
  }

  @Test
  fun `getById returns 200 with mapped fields when the request exists`() {
    every { dsrDeletionService.get(requestId) } returns
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = DsrDeletionService.emailHash(email),
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
      )

    val resp = controller.getById(requestId)
    assertEquals(200, resp.status.code)
    val body = resp.body()
    assertNotNull(body)
    assertEquals("PREVIEWED", body.status)
    assertEquals(email, body.email)
    assertEquals(datagrailId, body.datagrailId)
    assertEquals(oncallIssueNumber, body.oncallIssueNumber)
    assertEquals(email, body.manifest.get("target_email").asText())
    assertEquals(null, body.executionCounts)
  }

  @Test
  fun `getById includes execution counts after execute completes`() {
    every { dsrDeletionService.get(requestId) } returns
      DataSubjectDeletionRequest(
        id = requestId,
        email = datagrailId,
        emailHash = DsrDeletionService.emailHash(email),
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.completed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(redactedManifest()),
        executionCounts =
          """
          {
            "deleted_jobs_count": 17,
            "deleted_attempts_count": 34,
            "deleted_connections_count": 2,
            "tombstoned_user": true
          }
          """.trimIndent(),
      )

    val resp = controller.getById(requestId)
    assertEquals(200, resp.status.code)
    val body = resp.body()
    assertNotNull(body)
    assertEquals("COMPLETED", body.status)
    assertNotNull(body.executionCounts)
    assertEquals(17, body.executionCounts!!.get("deleted_jobs_count").asInt())
    assertEquals(34, body.executionCounts!!.get("deleted_attempts_count").asInt())
    assertEquals(2, body.executionCounts!!.get("deleted_connections_count").asInt())
    assertEquals(true, body.executionCounts!!.get("tombstoned_user").asBoolean())
  }

  @Test
  fun `listByEmail returns the rows the service produced`() {
    every { dsrDeletionService.listByEmail(email) } returns
      listOf(
        DataSubjectDeletionRequest(
          id = requestId,
          email = datagrailId,
          emailHash = DsrDeletionService.emailHash(email),
          datagrailId = datagrailId,
          status = DataSubjectDeletionStatus.completed,
          userId = userId,
          requestedBy = requestedBy,
          oncallIssueNumber = oncallIssueNumber,
          manifest = objectMapper.writeValueAsString(redactedManifest()),
        ),
      )

    val resp = controller.listByEmail(email)
    assertEquals(1, resp.requests.size)
    assertEquals("COMPLETED", resp.requests[0].status)
    assertEquals(datagrailId, resp.requests[0].email)
  }

  @Test
  fun `service exceptions propagate so controller error mapping can respond`() {
    every { dsrDeletionService.startExecution(any(), any(), any(), any(), any()) } throws
      DsrInvalidConfirmationException("nope")
    assertThrows<DsrInvalidConfirmationException> {
      controller.execute(
        requestId,
        DsrExecuteRequest(email, datagrailId, oncallIssueNumber, executedBy),
      )
    }

    every { dsrDeletionService.startExecution(any(), any(), any(), any(), any()) } throws
      DsrInvalidStateException("nope")
    assertThrows<DsrInvalidStateException> {
      controller.execute(
        requestId,
        DsrExecuteRequest(email, datagrailId, oncallIssueNumber, executedBy),
      )
    }

    every { dsrDeletionService.startExecution(any(), any(), any(), any(), any()) } throws
      DsrRequestNotFoundException("nope")
    assertThrows<DsrRequestNotFoundException> {
      controller.execute(
        requestId,
        DsrExecuteRequest(email, datagrailId, oncallIssueNumber, executedBy),
      )
    }

    every { dsrDeletionService.preview(any(), any(), any(), any()) } throws
      DsrUserNotFoundException("nope")
    assertThrows<DsrUserNotFoundException> {
      controller.preview(
        DsrPreviewRequest(email, datagrailId, oncallIssueNumber, requestedBy),
      )
    }
  }

  @Test
  fun `cancel delegates to service`() {
    every { dsrDeletionService.cancel(requestId, executedBy) } returns
      DataSubjectDeletionRequest(
        id = requestId,
        email = datagrailId,
        emailHash = DsrDeletionService.emailHash(email),
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.canceled,
        userId = userId,
        requestedBy = requestedBy,
        confirmedBy = executedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(redactedManifest()),
      )

    val resp = controller.cancel(requestId, DsrCancelRequest(canceledBy = executedBy))
    assertNotNull(resp)
    assertEquals("CANCELED", resp!!.status)
    assertEquals(datagrailId, resp.email)
  }

  private fun manifest(): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin"),
      workspaceIds = emptyList(),
      workspaceRefs = emptyList(),
      organizationIds = emptyList(),
      organizationRefs = emptyList(),
      connectionIds = emptyList(),
      connectionRefs = emptyList(),
      sourceIds = emptyList(),
      destinationIds = emptyList(),
      connectorBuilderProjectIds = emptyList(),
      permissionIds = emptyList(),
      authUsers = emptyList(),
      jobCount = 0L,
      attemptCount = 0L,
      keycloakUsers = emptyList(),
      temporalWorkflows = emptyList(),
    )

  private fun redactedManifest(): DsrManifest =
    manifest().copy(
      targetEmail = datagrailId,
      user = DsrManifest.ManifestUser(userId, datagrailId, datagrailId),
    )
}
