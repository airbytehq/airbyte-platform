/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.api.client.ApiException
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatTimeoutException
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadHeartbeatSender(
  private val workloadApiClient: WorkloadApiClient,
  private val replicationWorkerState: ReplicationWorkerState,
  private val destinationTimeoutMonitor: DestinationTimeoutMonitor,
  private val sourceTimeoutMonitor: HeartbeatMonitor,
  private val source: AirbyteSource,
  @Named("workloadHeartbeatInterval") private val heartbeatInterval: Duration,
  @Named("workloadHeartbeatTimeout") private val heartbeatTimeoutDuration: Duration,
  @Named("hardExitCallable") private val hardExitCallable: () -> Unit,
  @Named("workloadId") private val workloadId: String,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Named("attemptId") private val attempt: Int,
) {
  private var sourceIsHanging = false

  /**
   * Sends periodic heartbeat requests until cancellation, terminal error, or heartbeat timeout.
   *
   * @param heartbeatInterval the interval between heartbeat requests.
   * @param heartbeatTimeoutDuration the maximum allowed duration between successful heartbeats.
   * @param workloadId the workload identifier.
   */
  suspend fun sendHeartbeat() {
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

          /*
           * Only fail the job if the source heartbeat has timed out for a source that has not already finished.  This is
           * to allow destinations that take a long time (e.g. typing and deduping) to finish even after the source has
           * exited normally.
           */
          (sourceTimeoutMonitor.hasTimedOut() && !checkSourceFinishedWithTimeout()) -> {
            val e =
              HeartbeatTimeoutException(
                sourceTimeoutMonitor.heartbeatFreshnessThreshold.toMillis(),
                sourceTimeoutMonitor.timeSinceLastBeat.orElse(Duration.ZERO).toMillis(),
              )
            logger.warn(e) { "Source has timed out; failing the workload." }
            failWorkload(e)

            // BUG: this is a hack to work around a bug where the source reader hangs
            if (sourceIsHanging) {
              source.close()
            }

            break
          }

          else -> {
            logger.debug { "Sending workload heartbeat." }
            workloadApiClient.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
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
