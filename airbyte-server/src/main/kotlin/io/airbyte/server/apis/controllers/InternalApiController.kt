/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.temporal.ConnectionManagerUtils
import io.airbyte.server.apis.execute
import io.airbyte.workload.services.WorkloadService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.core.annotation.Introspected
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Internal-only API controller for administrative operations.
 * These endpoints are not part of the public API but are accessible via ingress.
 * Access requires ADMIN role.
 */
@Controller("/api/v1/internal")
@Context
@Secured(AuthRoleConstants.ADMIN)
open class InternalApiController(
  private val jobsHandler: JobsHandler,
  private val connectionManagerUtils: ConnectionManagerUtils,
  private val workloadService: WorkloadService,
) {
  /**
   * Force cleanup of connections in stuck state.
   *
   * This is a break-glass endpoint for recovering connections that are stuck with:
   * - Jobs/attempts marked as RUNNING but no actual work happening
   * - Workflows in unreachable state
   * - Workloads in non-terminal state
   *
   * For each connection, performs:
   * 1. Terminates the workflow (clears isRunning state)
   * 2. Fails all non-terminal jobs/attempts
   * 3. Fails all non-terminal workloads (cancel is reserved for user cancellations)
   *
   * After cleanup, the connection can start fresh syncs.
   *
   * USAGE & PERFORMANCE CONSIDERATIONS:
   * - Expected usage: Catastrophic failure recovery only - NOT for regular daily operations
   * - This is a true "break-glass" endpoint for unusual system failures (e.g., platform crash,
   *   database corruption, temporal workflow issues) that leave connections in irrecoverable states
   * - Typical usage should be rare: a few times per month at most, ideally never
   * - NOT optimized for high-frequency or automated use
   * - Performance: Workload queries are not indexed by connection_id, which means
   *   they perform JSONB extraction on all non-terminal workloads. At scale (10K+ workloads),
   *   this may take 100-300ms per connection.
   * - Best practice: Investigate root cause before using. This endpoint treats symptoms, not causes.
   * - Batch wisely: While the endpoint accepts multiple connection IDs, processing
   *   large batches (100+) should be avoided to prevent long-running requests
   */
  @Post("/force_cleanup_connections")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun forceCleanupConnections(
    @Body forceCleanupConnectionsRequestBody: ForceCleanupConnectionsRequestBody,
  ): ForceCleanupConnectionsResponse? =
    execute {
      log.warn { "Break-glass cleanup requested for ${forceCleanupConnectionsRequestBody.connectionIds.size} connections" }

      val results =
        forceCleanupConnectionsRequestBody.connectionIds.map { connectionId ->
          cleanupConnection(connectionId)
        }

      ForceCleanupConnectionsResponse(results = results)
    }

  private fun cleanupConnection(connectionId: UUID): ConnectionCleanupResult {
    val errors = mutableListOf<String>()

    try {
      log.info { "Cleaning up connection: $connectionId" }

      // Step 1: Terminate workflow
      connectionManagerUtils.safeTerminateWorkflow(
        connectionId,
        "Force cleanup by admin via internal API",
      )
      log.info { "Workflow terminated for connection: $connectionId" }
    } catch (e: Exception) {
      log.error(e) { "Failed to terminate workflow for connection: $connectionId" }
      errors.add("Workflow termination failed: ${e.message}")
    }

    try {
      // Step 2: Fail non-terminal jobs
      // Note: This also updates attempts to FAILED
      jobsHandler.failNonTerminalJobs(connectionId)
      log.info { "Non-terminal jobs failed for connection: $connectionId" }
    } catch (e: Exception) {
      log.error(e) { "Failed to cleanup jobs for connection: $connectionId" }
      errors.add("Job cleanup failed: ${e.message}")
    }

    try {
      // Step 3: Fail non-terminal workloads
      val workloadErrors = failNonTerminalWorkloads(connectionId)
      if (workloadErrors.isNotEmpty()) {
        errors.addAll(workloadErrors)
        log.warn { "Some workloads could not be marked as failed for connection: $connectionId" }
      } else {
        log.info { "All non-terminal workloads marked as failed for connection: $connectionId" }
      }
    } catch (e: Exception) {
      log.error(e) { "Failed to fail workloads for connection: $connectionId" }
      errors.add("Workload failure failed: ${e.message}")
    }

    val success = errors.isEmpty()
    log.info { "Cleanup completed for connection: $connectionId. Success: $success" }

    return ConnectionCleanupResult(
      connectionId = connectionId,
      success = success,
      errors = errors,
    )
  }

  private fun failNonTerminalWorkloads(connectionId: UUID): List<String> {
    // Get all non-terminal workloads for this connection via WorkloadService
    val connectionWorkloads = workloadService.getNonTerminalWorkloadsByConnection(connectionId)

    log.info { "Found ${connectionWorkloads.size} non-terminal workloads for connection: $connectionId" }

    val workloadErrors = mutableListOf<String>()

    // Fail each workload (not cancel - cancel is reserved for user cancellations)
    connectionWorkloads.forEach { workload ->
      try {
        workloadService.failWorkload(
          workloadId = workload.id,
          source = "internal_api",
          reason = "Force cleanup by admin via break-glass endpoint",
          dataplaneVersion = null,
        )
        log.info { "Failed workload: ${workload.id} for connection: $connectionId" }
      } catch (e: Exception) {
        log.error(e) { "Failed to fail workload: ${workload.id} for connection: $connectionId" }
        workloadErrors.add("Failed to fail workload ${workload.id}: ${e.message}")
      }
    }

    return workloadErrors
  }
}

/**
 * Request body for force cleanup connections endpoint.
 */
@Introspected
data class ForceCleanupConnectionsRequestBody(
  @JsonProperty("connection_ids")
  val connectionIds: List<UUID>,
)

/**
 * Response body for force cleanup connections endpoint.
 */
@Introspected
data class ForceCleanupConnectionsResponse(
  @JsonProperty("results")
  val results: List<ConnectionCleanupResult>,
)

/**
 * Result of cleanup operation for a single connection.
 */
@Introspected
data class ConnectionCleanupResult(
  @JsonProperty("connection_id")
  val connectionId: UUID,
  @JsonProperty("success")
  val success: Boolean,
  @JsonProperty("errors")
  val errors: List<String>,
)
