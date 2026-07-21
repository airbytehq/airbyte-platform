/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock

private const val WINDOW_MS = 5_000L

class ExpiringGaugeValueTest {
  @Test
  fun `reportableValue is NaN before any value is recorded`() {
    val clock = mockk<Clock>()
    assertTrue(ExpiringGaugeValue(clock, WINDOW_MS).reportableValue().isNaN())
  }

  @Test
  fun `reportableValue returns the value when recorded within the window`() {
    val clock = mockk<Clock>()
    every { clock.millis() } returnsMany listOf(1_000L, 3_000L) // record, then read 2s later (window 5s)
    val gauge = ExpiringGaugeValue(clock, WINDOW_MS)

    gauge.record(42.0)

    assertEquals(42.0, gauge.reportableValue())
  }

  @Test
  fun `reportableValue is NaN when the last record is older than the window`() {
    val clock = mockk<Clock>()
    every { clock.millis() } returnsMany listOf(1_000L, 10_000L) // record, then read 9s later (window 5s)
    val gauge = ExpiringGaugeValue(clock, WINDOW_MS)

    gauge.record(42.0)

    assertTrue(gauge.reportableValue().isNaN())
  }
}
