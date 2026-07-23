/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.duration

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class DurationTest {
  @ParameterizedTest
  @CsvSource(
    "-1000, 0 seconds",
    "0, 0 seconds",
    "1000, 1 second",
    "2000, 2 seconds",
    "60000, 1 minute",
    "67890, 1 minute 7 seconds",
    "600000, 10 minutes",
    "4567890, 1 hour 16 minutes 7 seconds",
    "34567890, 9 hours 36 minutes 7 seconds",
    "86400000, 1 day",
    "86401000, 1 day 0 hours 0 minutes 1 second",
    "123400000, 1 day 10 hours 16 minutes 40 seconds",
    "123420000, 1 day 10 hours 17 minutes",
    "234567890, 2 days 17 hours 9 minutes 27 seconds",
    "951654321, 11 days 0 hours 20 minutes 54 seconds",
    "987654321, 11 days 10 hours 20 minutes 54 seconds",
  )
  fun formatMilli(
    millis: Long,
    expected: String,
  ) {
    assertEquals(expected, formatMilli(millis))
  }
}
