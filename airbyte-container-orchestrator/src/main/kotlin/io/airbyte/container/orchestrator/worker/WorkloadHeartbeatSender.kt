/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.exception.WorkloadHeartbeatException
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatTimeoutException
import io.airbyte.featureflag.OrchestratorHardFailOnHeartbeatFailure
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
  private val flagReader: ReplicationInputFeatureFlagReader,
  @Named("workloadHeartbeatInterval") private val heartbeatInterval: Duration,
  @Named("workloadHeartbeatTimeout") private val heartbeatTimeoutDuration: Duration,
  @Named("hardExitCallable") private val hardExitCallable: () -> Unit,
  @Named("workloadId") private val workloadId: String,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Named("attemptId") private val attempt: Int,
) {
  @Deprecated("Delete and rename sendHeartbeatV2 to sendHeartbeat once migrated")
  suspend fun sendHeartbeat() {
    if (flagReader.read(OrchestratorHardFailOnHeartbeatFailure)) {
      sendHeartbeatV2()
    } else {
      sendHeartbeatFormer()
    }
  }

  /**
   * Sends periodic heartbeat requests until cancellation, terminal error, or heartbeat timeout.
   *
   * @param heartbeatInterval the interval between heartbeat requests.
   * @param heartbeatTimeoutDuration the maximum allowed duration between successful heartbeats.
   * @param workloadId the workload identifier.
   */
  @Deprecated("We want to migrate to sendHeartbeatV2")
  suspend fun sendHeartbeatFormer() {
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

          sourceTimeoutMonitor.hasTimedOut() -> {
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

  /**
   * Sends periodic heartbeat requests until cancellation, terminal error, or heartbeat timeout.
   *
   * @param heartbeatInterval the interval between heartbeat requests.
   * @param heartbeatTimeoutDuration the maximum allowed duration between successful heartbeats.
   * @param workloadId the workload identifier.
   */
  suspend fun sendHeartbeatV2() {
    logger.info { "Starting workload heartbeat (interval=${heartbeatInterval.seconds}s; timeout=${heartbeatTimeoutDuration.seconds}s)" }
    var lastSuccessfulHeartbeat = Instant.now()

    while (true) {
      // Capture current time at start of loop iteration.
      val now = Instant.now()
      try {
        when {
          destinationTimeoutMonitor.hasTimedOut() -> {
            val e =
              DestinationTimeoutMonitor.TimeoutException(
                destinationTimeoutMonitor.timeoutThresholdSec.toMillis(),
                destinationTimeoutMonitor.timeSinceLastAction.get(),
              )
            logger.warn(e) { "Destination has timed out; failing the workload." }
            failWorkload(e)
            break
          }

          sourceTimeoutMonitor.hasTimedOut() -> {
            val e =
              HeartbeatTimeoutException(
                sourceTimeoutMonitor.heartbeatFreshnessThreshold.toMillis(),
                sourceTimeoutMonitor.timeSinceLastBeat.orElse(Duration.ZERO).toMillis(),
              )
            logger.warn(e) { "Source has timed out; failing the workload." }
            failWorkload(e)
            break
          }

          else -> {
            logger.debug { "Sending workload heartbeat." }
            workloadApiClient.workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
            lastSuccessfulHeartbeat = now
          }
        }
      } catch (e: Exception) {
        when {
          // If the workload-api returns HttpStatus.GONE, this means that the workload has reached a terminal state. Most likely,
          // it has been canceled by the user or failed from the workload-api. Could be timeout from the workload monitor or workload
          // has been superseded by another workload.
          // In this case, the workload has already been failed, we should exit ASAP.
          e is ClientException && e.statusCode == HttpStatus.GONE.code -> {
            logger.warn(e) { "Workload in terminal state; exiting." }
            exitAsap()
            break
          }

          // For the other errors, if we fail to hearbeat successfully within the heartbeatTimeoutDuration, we assume we cannot
          // reach the control-plane and exit. This is to allow the control-plane to reschedule the workload on a different
          // data-plane while limiting the risk of duplicate in the event of a network partition for example.
          hasExpired(lastSuccessfulHeartbeat, heartbeatTimeoutDuration) -> {
            logger.error(e) { "No successful heartbeat in the last ${heartbeatTimeoutDuration.seconds}s; exiting." }
            exitAsap()
            break
          }

          else -> logger.warn(e) { "Error sending heartbeat" }
        }
      }
      delay(heartbeatInterval.toMillis())
    }
    logger.info { "Exiting the workload heartbeat task." }
  }

  private fun hasExpired(
    lastSuccessfulHeartbeat: Instant,
    timeoutDuration: Duration,
  ): Boolean = Duration.between(lastSuccessfulHeartbeat, Instant.now()) > timeoutDuration

  /**
   * Fail the current workload.
   */
  private fun failWorkload(throwable: Throwable) {
    replicationWorkerState.trackFailure(throwable, jobId, attempt)
    replicationWorkerState.markFailed()
    replicationWorkerState.abort()
  }

  /**
   * Hard exit, should be called in cases where we have been signaled to shut down from the outside.
   */
  private fun exitAsap() {
    hardExitCallable()
  }
}
