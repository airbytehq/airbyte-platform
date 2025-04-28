/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.config.FailureReason
import io.airbyte.workers.exception.WorkloadHeartbeatException
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.internal.DestinationTimeoutMonitor
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

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
    replicationFailures.add(getFailureReason(jobId, attempt, t))
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
      is HeartbeatTimeoutChaperone.HeartbeatTimeoutException ->
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
