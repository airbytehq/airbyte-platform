/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.duration.formatMilli
import io.airbyte.persistence.job.models.ReplicationInput
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import java.util.function.Supplier
import kotlin.Boolean
import kotlin.Long
import kotlin.RuntimeException
import kotlin.let

/**
 * Tracks heartbeats and, when asked, says if it has been too long since the last heartbeat. He's
 * dead Jim!
 *
 * It is ThreadSafe.
 */
@Singleton
class HeartbeatMonitor(
  val replicationInput: ReplicationInput,
  private val nowSupplier: Supplier<Instant>? = Supplier { Instant.now() },
) {
  val heartbeatFreshnessThreshold: Duration = Duration.ofSeconds(replicationInput.heartbeatConfig.maxSecondsBetweenMessages)
  private val lastBeat: AtomicReference<Instant> = AtomicReference<Instant>(null)

  @PostConstruct
  fun init() {
    nowSupplier?.let { supplier -> lastBeat.set(supplier.get()) }
  }

  /**
   * Register a heartbeat.
   */
  fun beat() {
    nowSupplier?.let { supplier -> lastBeat.set(supplier.get()) }
  }

  val isBeating: Optional<Boolean>
    /**
     * Verify if the heart is still beating.
     *
     * @return true if the last heartbeat is still "fresh". i.e. time since last heartbeat is less than
     * heartBeatFreshDuration. otherwise, false.
     */
    get() = this.timeSinceLastBeat.map(Function { timeSinceLastBeat: Duration -> timeSinceLastBeat < heartbeatFreshnessThreshold })

  val timeSinceLastBeat: Optional<Duration>
    /**
     * Return the time since the last beat. It returns empty is no beat has been performed.
     */
    get() {
      val instantFetched = lastBeat.get()

      return if (instantFetched == null) {
        Optional.empty<Duration>()
      } else {
        Optional.ofNullable<Duration>(Duration.between(lastBeat.get(), nowSupplier?.get()))
      }
    }
}

/**
 * Exception thrown is the timeout is not beating.
 */
class HeartbeatTimeoutException(
  thresholdMs: Long,
  timeBetweenLastRecordMs: Long,
) : RuntimeException(
    "Last record seen ${formatMilli(timeBetweenLastRecordMs)} ago, exceeding the threshold of ${formatMilli(thresholdMs)}.",
  ) {
  val humanReadableThreshold = formatMilli(thresholdMs)
  val humanReadableTimeSinceLastRec = formatMilli(timeBetweenLastRecordMs)
}
