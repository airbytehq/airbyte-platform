/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.duration.formatMilli
import io.airbyte.featureflag.DestinationTimeoutSeconds
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.lang.AutoCloseable
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}
private val POLL_INTERVAL: Duration = Duration.ofMinutes(1)

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
  private val pollInterval: Duration = POLL_INTERVAL,
) : AutoCloseable {
  private val currentAcceptCallStartTime = AtomicLong(-1)
  private val currentNotifyEndOfInputCallStartTime = AtomicLong(-1)
  val timeSinceLastAction: AtomicLong = AtomicLong(-1)
  private var lazyExecutorService: ExecutorService? = null
  val timeoutThresholdSec: Duration =
    Duration.ofSeconds(
      replicationInputFeatureFlagReader.read(DestinationTimeoutSeconds).toLong(),
    )

  /**
   * Keeps track of two tasks:
   *
   * 1. The given runnableFuture
   *
   * 2. A timeoutMonitorFuture that is created within this method
   *
   * This method completes when either of the above completes.
   *
   * The timeoutMonitorFuture checks if there has been a timeout on either an
   * [io.airbyte.workers.internal.AirbyteDestination.accept] call or a
   * [io.airbyte.workers.internal.AirbyteDestination.notifyEndOfInput] call. If runnableFuture completes before the
   * timeoutMonitorFuture, then the timeoutMonitorFuture will be canceled. If there's a timeout before
   * the runnableFuture completes, then the runnableFuture will be canceled and this method will throw
   * a [TimeoutException] (assuming the [io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout] feature flag
   * returned true, otherwise this method will complete without throwing an exception, the runnable
   * won't be canceled and the timeoutMonitorFuture will be canceled).
   *
   * notifyEndOfInput/accept calls timeouts are tracked by calling
   * [.startNotifyEndOfInputTimer], [.resetNotifyEndOfInputTimer],
   * [.startAcceptTimer] and [.resetAcceptTimer].
   *
   * Note that there are three tasks involved in this method:
   *
   * 1. The given runnableFuture
   *
   * 2. A timeoutMonitorFuture that is created within this method
   *
   * 3. The task that waits for the above two tasks to complete
   *
   */
  @Throws(ExecutionException::class)
  fun runWithTimeoutThread(runnableFuture: CompletableFuture<Void?>) {
    val timeoutMonitorFuture = CompletableFuture.runAsync({ this.pollForTimeout() }, getLazyExecutorService())

    try {
      CompletableFuture.anyOf(runnableFuture, timeoutMonitorFuture).get()
    } catch (e: InterruptedException) {
      logger.error(e) { "Timeout thread was interrupted." }
      return
    } catch (e: ExecutionException) {
      if (e.cause is RuntimeException) {
        throw e.cause as RuntimeException
      } else {
        throw e
      }
    }

    logger.info { "thread status... timeout thread: ${timeoutMonitorFuture.isDone} , replication thread: ${runnableFuture.isDone}" }

    if (timeoutMonitorFuture.isDone && !runnableFuture.isDone) {
      onTimeout(runnableFuture, timeoutThresholdSec.toMillis(), timeSinceLastAction.get())
    }

    timeoutMonitorFuture.cancel(true)
  }

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

  private fun onTimeout(
    runnableFuture: CompletableFuture<Void?>,
    threshold: Long,
    timeSinceLastAction: Long,
  ) {
    if (replicationInputFeatureFlagReader.read(ShouldFailSyncOnDestinationTimeout)) {
      runnableFuture.cancel(true)

      throw TimeoutException(threshold, timeSinceLastAction)
    } else {
      logger.info {
        "Destination has timed out but exception is not thrown due to feature flag being disabled for workspace ${replicationInput.workspaceId} and connection ${replicationInput.connectionId}."
      }
    }
  }

  private fun pollForTimeout() {
    while (true) {
      try {
        Thread.sleep(pollInterval.toMillis())
      } catch (e: InterruptedException) {
        logger.info(e) { "Stopping timeout monitor" }
        return
      }

      if (hasTimedOut()) {
        return
      }
    }
  }

  fun hasTimedOut(): Boolean {
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
    lazyExecutorService?.let { service ->
      service.shutdownNow()
      try {
        service.awaitTermination(10, TimeUnit.SECONDS)
      } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
      }
    }
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

  /**
   * Return an executor service which is initialized in a lazy way.
   */
  private fun getLazyExecutorService(): ExecutorService {
    if (lazyExecutorService == null) {
      lazyExecutorService = Executors.newFixedThreadPool(1)
    }

    return lazyExecutorService!!
  }
}
