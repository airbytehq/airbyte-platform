/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.config.FailureReason
import io.airbyte.container.orchestrator.worker.exception.WorkloadHeartbeatException
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatTimeoutException
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.helper.MAX_FAILURES_TO_KEEP
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

@Singleton
class ReplicationWorkerState {
  private val replicationFailures = Collections.synchronizedList(mutableListOf<FailureReason>())

  private val _cancelled = AtomicBoolean(false)
  private val _hasFailed = AtomicBoolean(false)
  private val _shouldAbort = AtomicBoolean(false)
  val cancelled: Boolean get() = _cancelled.get()
  val hasFailed: Boolean get() = _hasFailed.get()
  val shouldAbort: Boolean get() = _shouldAbort.get() || _cancelled.get()

  fun markCancelled() {
    _cancelled.set(true)
  }

  fun markFailed() {
    _hasFailed.set(true)
  }

  fun abort() {
    _shouldAbort.set(true)
  }

  fun markReplicationRunning(onReplicationRunning: VoidCallable) {
    onReplicationRunning.call()
  }

  fun trackFailure(
    t: Throwable,
    jobId: Long,
    attempt: Int,
  ) {
    when (replicationFailures.size) {
      in 0 until MAX_FAILURES_TO_KEEP -> {
        replicationFailures += getFailureReason(jobId, attempt, t)
      }
      MAX_FAILURES_TO_KEEP -> {
        logger.warn(
          t,
        ) { "Reached failure limit ($MAX_FAILURES_TO_KEEP). Adding truncation notice. Additional failures will be logged but not stored." }
        replicationFailures +=
          FailureReason().apply {
            failureType = FailureReason.FailureType.SYSTEM_ERROR
            failureOrigin = FailureReason.FailureOrigin.AIRBYTE_PLATFORM
            internalMessage = "Truncated additional failures to prevent serialization issues"
            externalMessage = "Additional failures were truncated for performance reasons. Check logs for complete failure details."
            timestamp = System.currentTimeMillis()
          }
      }
      else -> {
        logger.warn(
          t,
        ) { "Failure ignored due to limit ($MAX_FAILURES_TO_KEEP failures already stored). Job: $jobId, Attempt: $attempt, Error: ${t.message}" }
      }
    }
  }

  fun getFailures(): List<FailureReason> = replicationFailures

  private fun getFailureReason(
    jobId: Long,
    attempt: Int,
    ex: Throwable,
  ): FailureReason =
    when (ex) {
      is SourceException -> FailureHelper.sourceFailure(ex, jobId, attempt)
      is DestinationException -> FailureHelper.destinationFailure(ex, jobId, attempt)
      is HeartbeatTimeoutException ->
        FailureHelper.sourceHeartbeatFailure(
          ex,
          jobId,
          attempt,
          ex.humanReadableThreshold,
          ex.humanReadableTimeSinceLastRec,
        )

      is DestinationTimeoutMonitor.TimeoutException ->
        FailureHelper.destinationTimeoutFailure(
          ex,
          jobId,
          attempt,
          ex.humanReadableThreshold,
          ex.humanReadableTimeSinceLastAction,
        )

      is WorkloadHeartbeatException -> FailureHelper.platformFailure(ex, jobId, attempt, ex.message)
      else -> FailureHelper.replicationFailure(ex, jobId, attempt)
    }
}
