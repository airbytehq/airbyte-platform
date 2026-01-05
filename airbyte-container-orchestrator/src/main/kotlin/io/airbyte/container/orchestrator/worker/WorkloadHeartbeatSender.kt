/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.api.client.ApiException
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatTimeoutException
import io.airbyte.featureflag.HeartbeatDiagnosticLogsEnabled
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

// Log every 100 iterations (~1000s with default 10s interval) when diagnostic logging is enabled
private const val HEARTBEAT_STATUS_LOG_INTERVAL = 100L

@Singleton
class WorkloadHeartbeatSender(
  private val workloadApiClient: WorkloadApiClient,
  private val replicationWorkerState: ReplicationWorkerState,
  private val destinationTimeoutMonitor: DestinationTimeoutMonitor,
  private val sourceTimeoutMonitor: HeartbeatMonitor,
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  @Named("workloadHeartbeatInterval") private val heartbeatInterval: Duration,
  @Named("workloadHeartbeatTimeout") private val heartbeatTimeoutDuration: Duration,
  @Named("hardExitCallable") private val hardExitCallable: () -> Unit,
  private val airbyteContextConfig: AirbyteContextConfig,
  replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
) {
  private var sourceIsHanging = false
  private val diagnosticLogsEnabled = replicationInputFeatureFlagReader.read(HeartbeatDiagnosticLogsEnabled)

  /**
   * Sends periodic heartbeat requests until cancellation, terminal error, or heartbeat timeout.
   */
  suspend fun sendHeartbeat() {
    logger.info { "Starting workload heartbeat (interval=${heartbeatInterval.seconds}s; timeout=${heartbeatTimeoutDuration.seconds}s)" }
    var lastSuccessfulHeartbeat = Instant.now()

    // Transition workload to running state before sending heartbeats
    // This must be called once before the heartbeat loop starts
    var runningTransitionSucceeded = false
    while (!runningTransitionSucceeded) {
      try {
        logger.info { "Transitioning workload to running state" }
        workloadApiClient.workloadRunning(WorkloadRunningRequest(airbyteContextConfig.workloadId))
        logger.info { "Workload successfully transitioned to running state" }
        runningTransitionSucceeded = true
        lastSuccessfulHeartbeat = Instant.now()
      } catch (e: Exception) {
        when {
          // If GONE, workload is in terminal state - exit immediately
          e is ApiException && e.statusCode == HttpStatus.GONE.code -> {
            logger.warn(e) { "Workload in terminal state; exiting." }
            exitAsap()
            return
          }
          // Give same grace period as heartbeats for non-GONE errors
          hasExpired(lastSuccessfulHeartbeat, heartbeatTimeoutDuration) -> {
            logger.error(e) { "Failed to transition workload to running state within ${heartbeatTimeoutDuration.seconds}s; exiting." }
            exitAsap()
            return
          }
          else -> {
            logger.warn(e) { "Error transitioning to running state, retrying in ${heartbeatInterval.seconds}s" }
            delay(heartbeatInterval.toMillis())
          }
        }
      }
    }

    var heartbeatLoopIteration = 0L
    while (true) {
      // Capture current time at start of loop iteration.
      val now = Instant.now()
      heartbeatLoopIteration++

      // Log source heartbeat status periodically when diagnostic logs are enabled
      if (diagnosticLogsEnabled && heartbeatLoopIteration % HEARTBEAT_STATUS_LOG_INTERVAL == 0L) {
        val timeSinceLastBeat = sourceTimeoutMonitor.timeSinceLastBeat.orElse(null)
        val threshold = sourceTimeoutMonitor.heartbeatFreshnessThreshold
        logger.info {
          "Source heartbeat status: timeSinceLastBeat=${timeSinceLastBeat?.toSeconds()}s, " +
            "threshold=${threshold.toSeconds()}s, hasTimedOut=${sourceTimeoutMonitor.hasTimedOut()}"
        }
      }

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

            // Close the destination to unblock any threads stuck reading from destination pipes.
            // The DestinationReader may be blocked in a native read0() call on the stdout FIFO,
            // which ignores Thread.interrupt() and coroutine cancellation. Closing the destination
            // triggers ContainerIOHandle.terminate() which closes the underlying streams, causing
            // the blocked read to throw IOException and allowing shutdown to proceed.
            destination.close()

            break
          }

          /*
           * Only fail the job if the source heartbeat has timed out for a source that has not already finished.  This is
           * to allow destinations that take a long time (e.g. typing and deduping) to finish even after the source has
           * exited normally.
           */
          (sourceTimeoutMonitor.hasTimedOut() && !checkSourceFinishedWithTimeout()) -> {
            val timeSinceLastBeat = sourceTimeoutMonitor.timeSinceLastBeat.orElse(Duration.ZERO)
            val threshold = sourceTimeoutMonitor.heartbeatFreshnessThreshold
            // Always log timeout detection since it's an error condition
            logger.error {
              "Source heartbeat timeout detected: timeSinceLastBeat=${timeSinceLastBeat.toSeconds()}s, " +
                "threshold=${threshold.toSeconds()}s, sourceIsFinished=false"
            }
            val e =
              HeartbeatTimeoutException(
                threshold.toMillis(),
                timeSinceLastBeat.toMillis(),
              )
            logger.warn(e) { "Source has timed out; failing the workload." }
            failWorkload(e)

            // Close the source to unblock any threads stuck reading from source pipes.
            // The SourceReader may be blocked in a native read0() call on the stdout FIFO,
            // which ignores Thread.interrupt() and coroutine cancellation. Closing the source
            // triggers ContainerIOHandle.terminate() which closes the underlying streams, causing
            // the blocked read to throw IOException and allowing shutdown to proceed.
            // We only do this if sourceIsHanging is true, which indicates that checking
            // source.isFinished itself timed out (meaning the source is definitely stuck).
            if (sourceIsHanging) {
              source.close()
            }

            break
          }

          else -> {
            logger.debug { "Sending workload heartbeat." }
            workloadApiClient.workloadHeartbeat(WorkloadHeartbeatRequest(airbyteContextConfig.workloadId))
            lastSuccessfulHeartbeat = now
          }
        }
      } catch (e: Exception) {
        when {
          // If the workload-api returns HttpStatus.GONE, this means that the workload has reached a terminal state. Most likely,
          // it has been canceled by the user or failed from the workload-api. Could be timeout from the workload monitor or workload
          // has been superseded by another workload.
          // In this case, the workload has already been failed, we should exit ASAP.
          e is ApiException && e.statusCode == HttpStatus.GONE.code -> {
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
   * Checks if source is finished with a 1-minute timeout.
   * Returns false if the call doesn't complete within 1 minute.
   * This runs completely outside the coroutine context to avoid cancellation issues.
   */
  private fun checkSourceFinishedWithTimeout(): Boolean =
    try {
      val future =
        CompletableFuture.supplyAsync {
          source.isFinished
        }

      try {
        future.get(60, TimeUnit.SECONDS)
      } catch (_: Exception) {
        logger.warn { "Checking source.isFinished timed out after 1 minute, returning false" }
        future.cancel(true)
        sourceIsHanging = true
        false
      }
    } catch (e: Exception) {
      logger.warn(e) { "Exception occurred while checking source finished state, returning false" }
      false
    }

  /**
   * Fail the current workload.
   */
  private fun failWorkload(throwable: Throwable) {
    replicationWorkerState.trackFailure(throwable, airbyteContextConfig.jobId, airbyteContextConfig.attemptId)
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
