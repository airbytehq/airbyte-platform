/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.duration.formatMilli
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.featureflag.DestinationTimeoutSeconds
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.ReplicationInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.lang.AutoCloseable
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Tracks timeouts on [io.airbyte.workers.internal.AirbyteDestination.accept] and
 * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] calls.
 *
 * Starts monitoring timeouts when calling [.runWithTimeoutThread], which is meant to be
 * running as a background task while calls to
 * [io.airbyte.workers.internal.AirbyteDestination.accept] and
 * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] are being made.
 *
 * notifyEndOfInput/accept calls timeouts are tracked by calling
 * [.startNotifyEndOfInputTimer], [.resetNotifyEndOfInputTimer],
 * [.startAcceptTimer] and [.resetAcceptTimer]. These methods would be considered as
 * Timed out when either timer goes over [.timeout].
 *
 * The monitor checks for a timeout every [.pollInterval].
 */
@Singleton
class DestinationTimeoutMonitor(
  private val replicationInput: ReplicationInput,
  private val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  private val metricClient: MetricClient,
) : AutoCloseable {
  private val currentAcceptCallStartTime = AtomicLong(-1)
  private val currentNotifyEndOfInputCallStartTime = AtomicLong(-1)
  private val timeOutEnabled = replicationInputFeatureFlagReader.read(ShouldFailSyncOnDestinationTimeout)
  val timeSinceLastAction: AtomicLong = AtomicLong(-1)
  val timeoutThresholdSec: Duration =
    Duration.ofSeconds(
      replicationInputFeatureFlagReader.read(DestinationTimeoutSeconds).toLong(),
    )

  /**
   * Use to start a timeout timer on a call to
   * [io.airbyte.workers.internal.AirbyteDestination.accept]. For each call to
   * [.startAcceptTimer] there should be a corresponding call to [.resetAcceptTimer] to
   * stop the timeout timer.
   *
   * Only one [io.airbyte.workers.internal.AirbyteDestination.accept] call can be tracked at a
   * time. If there's an active [io.airbyte.workers.internal.AirbyteDestination.accept] call
   * being tracked and a call to [.startAcceptTimer] is done before a call to
   * [.resetAcceptTimer], the timer will start over, ignoring the time of spent on the first
   * [io.airbyte.workers.internal.AirbyteDestination.accept] call.
   */
  fun startAcceptTimer() {
    currentAcceptCallStartTime.set(System.currentTimeMillis())
  }

  /**
   * Use to stop the timeout timer on a call to
   * [io.airbyte.workers.internal.AirbyteDestination.accept]. Calling this method only makes
   * sense if there's a previous call to [.startAcceptTimer].
   */
  fun resetAcceptTimer() {
    currentAcceptCallStartTime.set(-1)
  }

  /**
   * Use to start a timeout timer on a call to
   * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput]. For each call to
   * [.startNotifyEndOfInputTimer] there should be a corresponding call to
   * [.resetNotifyEndOfInputTimer] to stop the timeout timer.
   *
   * Only one [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] call can be
   * tracked at a time. If there's an active
   * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] call being tracked and a
   * call to [.startNotifyEndOfInputTimer] is done before a call to
   * [.resetNotifyEndOfInputTimer], the timer will start over, ignoring the time of spent on the
   * first [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] call.
   */
  fun startNotifyEndOfInputTimer() {
    currentNotifyEndOfInputCallStartTime.set(System.currentTimeMillis())
  }

  /**
   * Use to stop the timeout timer on a call to
   * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput]. Calling this method only
   * makes sense if there's a previous call to [.startNotifyEndOfInputTimer].
   */
  fun resetNotifyEndOfInputTimer() {
    currentNotifyEndOfInputCallStartTime.set(-1)
  }

  fun hasTimedOut(): Boolean {
    if (!timeOutEnabled) {
      return false
    }
    if (hasTimedOutOnAccept()) {
      return true
    }
    return hasTimedOutOnNotifyEndOfInput()
  }

  private fun hasTimedOutOnAccept(): Boolean {
    val startTime = currentAcceptCallStartTime.get()

    if (startTime != -1L) {
      // by the time we get here, currentAcceptCallStartTime might have already been reset.
      // this won't be a problem since we are not getting the start time from currentAcceptCallStartTime
      // but from startTime
      val timeSince = System.currentTimeMillis() - startTime
      if (timeSince > timeoutThresholdSec.toMillis()) {
        logger.error { "Destination has timed out on accept call" }
        metricClient.count(
          metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT,
          attributes = arrayOf(MetricAttribute(MetricTags.CONNECTION_ID, replicationInput.connectionId.toString())),
        )
        timeSinceLastAction.set(timeSince)
        return true
      }
    }
    return false
  }

  private fun hasTimedOutOnNotifyEndOfInput(): Boolean {
    val startTime = currentNotifyEndOfInputCallStartTime.get()

    if (startTime != -1L) {
      // by the time we get here, currentNotifyEndOfInputCallStartTime might have already been reset.
      // this won't be a problem since we are not getting the start time from
      // currentNotifyEndOfInputCallStartTime but from startTime
      val timeSince = System.currentTimeMillis() - startTime
      if (timeSince > timeoutThresholdSec.toMillis()) {
        logger.error { "Destination has timed out on notifyEndOfInput call" }
        metricClient.count(
          metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
          attributes = arrayOf(MetricAttribute(MetricTags.CONNECTION_ID, replicationInput.connectionId.toString())),
        )
        timeSinceLastAction.set(timeSince)
        return true
      }
    }

    return false
  }

  @Throws(Exception::class)
  override fun close() {
  }

  class TimeoutException(
    thresholdMs: Long,
    timeSinceLastActionMs: Long,
  ) : RuntimeException(
      String.format(
        "Last action %s ago, exceeding the threshold of %s.",
        formatMilli(timeSinceLastActionMs),
        formatMilli(thresholdMs),
      ),
    ) {
    val humanReadableThreshold: String = formatMilli(thresholdMs)
    val humanReadableTimeSinceLastAction: String = formatMilli(timeSinceLastActionMs)
  }
}
