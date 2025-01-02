/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal.scheduling.retries

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Duration
import kotlin.math.min
import kotlin.math.pow

private val DEFAULT_BASE: Long = 2

/**
 * Calculates how much we should wait based off the provided configuration. Uses exponential backoff
 * clamped to a min/max interval.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class BackoffPolicy(
  val minInterval: Duration = Duration.ofSeconds(10),
  val maxInterval: Duration = Duration.ofHours(1),
  val base: Long = DEFAULT_BASE,
) {
  constructor(minInterval: Duration, maxInterval: Duration) : this(minInterval = minInterval, maxInterval = maxInterval, base = DEFAULT_BASE)

  /**
   * Calculates backoff based off provided steps.
   *
   * @param ordinal the number of adverse effects we have observed.
   * @return backoff duration to wait.
   */
  fun getBackoff(ordinal: Long): Duration {
    if (ordinal < 1) {
      return Duration.ZERO
    }

    val coefficient = base.toDouble().pow((ordinal - 1).toDouble())
    val calculated = (minInterval.toMillis() * coefficient).toLong()
    val clamped = min(calculated.toDouble(), maxInterval.toMillis().toDouble()).toLong()

    return Duration.ofMillis(clamped)
  }
}
