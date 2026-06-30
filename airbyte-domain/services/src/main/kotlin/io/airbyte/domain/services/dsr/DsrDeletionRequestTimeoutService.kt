/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dsr

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
open class DsrDeletionRequestTimeoutService(
  private val deletionRequestRepository: DataSubjectDeletionRequestRepository,
  private val objectMapper: ObjectMapper,
) {
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
  data class ExecutionCounts(
    val deletedJobsCount: Int = 0,
    val deletedAttemptsCount: Int = 0,
    val deletedConnectionsCount: Int = 0,
    val deletedActorsCount: Int = 0,
    val deletedBuilderProjectsCount: Int = 0,
    val deletedPermissionsCount: Int = 0,
    val deletedWorkspacesCount: Int = 0,
    val deletedOrganizationsCount: Int = 0,
    val deletedAuthUsersCount: Int = 0,
    val tombstonedUser: Boolean = false,
    val deletedKeycloakUserCount: Int = 0,
    val terminatedTemporalWorkflowCount: Int = 0,
  )

  data class TimeoutRecoveryResult(
    val activeTimedOutCount: Int,
    val activeFailedCount: Int,
    val queuedTimedOutCount: Int,
    val queuedRecoveredCount: Int,
  ) {
    val recoveredCount: Int
      get() = activeFailedCount + queuedRecoveredCount
  }

  open fun failTimedOutActiveRequest(
    requestId: UUID,
    executionTimeout: Duration,
  ): Boolean {
    val row = deletionRequestRepository.findById(requestId).orElse(null) ?: return false
    if (row.status != DataSubjectDeletionStatus.running) {
      return false
    }
    return failTimedOutActiveRequest(row, staleBefore(executionTimeout), executionTimeout)
  }

  open fun recoverTimedOutRunningRequests(executionTimeout: Duration): Int =
    recoverTimedOutRunningRequestsWithSummary(executionTimeout).recoveredCount

  open fun recoverTimedOutRunningRequestsWithSummary(executionTimeout: Duration): TimeoutRecoveryResult {
    val staleBefore = staleBefore(executionTimeout)
    val activeTimedOutRequests = deletionRequestRepository.findRunningUpdatedBefore(staleBefore)
    val queuedTimedOutRequests = deletionRequestRepository.findQueuedRunningUpdatedBefore(staleBefore)
    log.info {
      "DSR timeout recovery found ${activeTimedOutRequests.size} active and ${queuedTimedOutRequests.size} queued " +
        "RUNNING requests older than $staleBefore (timeout=$executionTimeout)"
    }

    val activeFailedCount = activeTimedOutRequests.count { failTimedOutActiveRequest(it, staleBefore, executionTimeout) }
    val queuedRecoveredCount = queuedTimedOutRequests.count { resetTimedOutQueuedRequest(it, staleBefore) }
    val result =
      TimeoutRecoveryResult(
        activeTimedOutCount = activeTimedOutRequests.size,
        activeFailedCount = activeFailedCount,
        queuedTimedOutCount = queuedTimedOutRequests.size,
        queuedRecoveredCount = queuedRecoveredCount,
      )
    log.warn {
      "DSR timeout recovery completed: activeTimedOut=${result.activeTimedOutCount}, " +
        "activeFailed=${result.activeFailedCount}, queuedTimedOut=${result.queuedTimedOutCount}, " +
        "queuedRecovered=${result.queuedRecoveredCount}"
    }
    return result
  }

  private fun failTimedOutActiveRequest(
    row: DataSubjectDeletionRequest,
    staleBefore: OffsetDateTime,
    executionTimeout: Duration,
  ): Boolean {
    val requestId = row.id ?: return false
    val manifest = parseManifest(row)
    val timeoutError =
      "Execution timed out after $executionTimeout. The background worker may have been interrupted; " +
        "verify no worker is still running before retrying."
    val updatedRows =
      deletionRequestRepository.failRunningIfTimedOut(
        requestId = requestId,
        staleBefore = staleBefore,
        completedAt = OffsetDateTime.now(ZoneOffset.UTC),
        scrubbedEmail = row.datagrailId,
        scrubbedManifest = objectMapper.writeValueAsString(redactedManifest(manifest, row.datagrailId)),
        confirmErrors = objectMapper.writeValueAsString(listOf(timeoutError)),
        executionCounts = objectMapper.writeValueAsString(ExecutionCounts()),
      )
    if (updatedRows == 1) {
      log.warn {
        "DSR timeout recovery marked active request $requestId FAILED after $executionTimeout; " +
          "lastUpdatedAt=${row.updatedAt}, staleBefore=$staleBefore"
      }
      return true
    }
    log.info {
      "DSR timeout recovery skipped active request $requestId because it was updated by another worker; " +
        "lastUpdatedAt=${row.updatedAt}, staleBefore=$staleBefore"
    }
    return false
  }

  private fun resetTimedOutQueuedRequest(
    row: DataSubjectDeletionRequest,
    staleBefore: OffsetDateTime,
  ): Boolean {
    val requestId = row.id ?: return false
    val updatedRows = deletionRequestRepository.markPreviewedIfQueuedTimedOut(requestId, staleBefore)
    if (updatedRows == 1) {
      log.warn {
        "DSR timeout recovery returned queued request $requestId to PREVIEWED; " +
          "lastUpdatedAt=${row.updatedAt}, staleBefore=$staleBefore"
      }
      return true
    }
    log.info {
      "DSR timeout recovery skipped queued request $requestId because it was updated by another worker; " +
        "lastUpdatedAt=${row.updatedAt}, staleBefore=$staleBefore"
    }
    return false
  }

  private fun staleBefore(executionTimeout: Duration): OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC).minus(executionTimeout)

  private fun parseManifest(row: DataSubjectDeletionRequest): DsrManifest =
    runCatching {
      objectMapper.readValue<DsrManifest>(row.manifest)
    }.getOrElse {
      DsrManifest(
        targetEmail = row.email,
        datagrailId = row.datagrailId,
        userId = row.userId ?: UUID(0L, 0L),
        user = row.userId?.let { userId -> DsrManifest.ManifestUser(userId, row.email, null) },
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
    }

  private fun redactedManifest(
    manifest: DsrManifest,
    datagrailId: String,
  ): DsrManifest =
    manifest.copy(
      targetEmail = datagrailId,
      user = manifest.user?.copy(email = datagrailId, name = datagrailId),
      workspaceRefs = manifest.workspaceRefs.map { it.copy(name = null) },
      organizationRefs = manifest.organizationRefs.map { it.copy(name = null) },
      connectionRefs = manifest.connectionRefs.map { it.copy(sourceName = null, destinationName = null) },
      keycloakUsers = manifest.keycloakUsers.map { it.copy(email = null, username = null) },
    )
}
