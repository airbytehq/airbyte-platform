/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import java.time.Clock
import java.util.concurrent.atomic.AtomicReference

/**
 * A gauge value that self-expires: [reportableValue] returns the most recently [record]ed value, or NaN once nothing
 * has been recorded within [windowMs]. Micrometer skips NaN gauge readings, so a stale value stops being published.
 */
class ExpiringGaugeValue(
  private val clock: Clock,
  private val windowMs: Long,
) {
  private val value = AtomicReference(Double.NaN)
  private val lastRecordedAtMs = AtomicReference<Long?>(null)

  fun record(newValue: Double) {
    value.set(newValue)
    lastRecordedAtMs.set(clock.millis())
  }

  fun reportableValue(): Double {
    val lastRecorded = lastRecordedAtMs.get() ?: return Double.NaN
    return if (clock.millis() - lastRecorded > windowMs) Double.NaN else value.get()
  }
}
