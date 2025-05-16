/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.exception.WorkloadHeartbeatException
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatTimeoutException
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.infrastructure.ClientException
import io.airbyte.workload.api.client.model.generated.WorkloadHeartbeatRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadHeartbeatSender(
  private val workloadApiClient: WorkloadApiClient,
  private val replicationWorkerState: ReplicationWorkerState,
  private val destinationTimeoutMonitor: DestinationTimeoutMonitor,
  private val sourceTimeoutMonitor: HeartbeatMonitor,
  private val heartbeatInterval: Duration,
  private val heartbeatTimeoutDuration: Duration,
  @Named("workloadId") private val workloadId: String,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Named("attemptId") private val attempt: Int,
) {
  /**
   * Sends periodic heartbeat requests until cancellation, terminal error, or heartbeat timeout.
   *
   * @param heartbeatInterval the interval between heartbeat requests.
   * @param heartbeatTimeoutDuration the maximum allowed duration between successful heartbeats.
   * @param workloadId the workload identifier.
   */
  suspend fun sendHeartbeat() {
    logger.info { "Starting workload heartbeat" }
    var lastSuccessfulHeartbeat = Instant.now()

    while (true) {
      // Capture current time at start of loop iteration.
      val now = Instant.now()
      try {
        when {
          destinationTimeoutMonitor.hasTimedOut() -> {
            logger.warn { "Destination has timed out; skipping heartbeat." }
            if (checkIfExpiredAndMarkSyncStateAsFailed(
                lastSuccessfulHeartbeat,
                heartbeatTimeoutDuration,
                DestinationTimeoutMonitor.TimeoutException(
                  destinationTimeoutMonitor.timeoutThresholdSec.toMillis(),
                  destinationTimeoutMonitor.timeSinceLastAction.get(),
                ),
              )
            ) {
              break
            }
          }

          !sourceTimeoutMonitor.isBeating.orElse(true) -> {
            logger.warn { "Source heartbeat missing; skipping heartbeat." }
            if (checkIfExpiredAndMarkSyncStateAsFailed(
                lastSuccessfulHeartbeat,
                heartbeatTimeoutDuration,
                HeartbeatTimeoutException(
                  sourceTimeoutMonitor.heartbeatFreshnessThreshold.toMillis(),
                  sourceTimeoutMonitor.timeSinceLastBeat.orElse(Duration.ZERO).toMillis(),
                ),
              )
            ) {
              break
            }
          }

          else -> {
            logger.debug { "Sending workload heartbeat" }
            workloadApiClient.workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
            lastSuccessfulHeartbeat = now
          }
        }
      } catch (e: Exception) {
        when {
          e is ClientException && e.statusCode == HttpStatus.GONE.code -> {
            logger.warn(e) { "Workload in terminal state; cancelling sync." }
            replicationWorkerState.markCancelled()
            break
          }

          checkIfExpiredAndMarkSyncStateAsFailed(
            lastSuccessfulHeartbeat,
            heartbeatTimeoutDuration,
            WorkloadHeartbeatException("Workload Heartbeat Error", e),
          ) -> break
          else -> logger.warn(e) { "Error sending heartbeat; retrying." }
        }
      }
      delay(heartbeatInterval.toMillis())
    }
  }

  /**
   * Checks if the time since the last successful heartbeat exceeds the allowed timeout.
   * If so, marks the replication as failed, aborts it, tracks the failure, and returns true.
   * Otherwise, returns false.
   */
  private fun checkIfExpiredAndMarkSyncStateAsFailed(
    lastSuccessfulHeartbeat: Instant,
    heartbeatTimeoutDuration: Duration,
    exception: Throwable,
  ): Boolean =
    if (Duration.between(lastSuccessfulHeartbeat, Instant.now()) > heartbeatTimeoutDuration) {
      logger.warn(exception) { "Heartbeat timeout exceeded. Shutting down heartbeat." }
      replicationWorkerState.trackFailure(exception, jobId, attempt)
      replicationWorkerState.markFailed()
      replicationWorkerState.abort()
      true
    } else {
      false
    }
}
