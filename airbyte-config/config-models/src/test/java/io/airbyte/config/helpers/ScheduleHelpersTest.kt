/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.Schedule
import io.airbyte.config.helpers.ScheduleHelpers.getSecondsInUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Arrays

internal class ScheduleHelpersTest {
  @Test
  fun testGetSecondsInUnit() {
    Assertions.assertEquals(60, getSecondsInUnit(Schedule.TimeUnit.MINUTES))
    Assertions.assertEquals(3600, getSecondsInUnit(Schedule.TimeUnit.HOURS))
    Assertions.assertEquals(86400, getSecondsInUnit(Schedule.TimeUnit.DAYS))
    Assertions.assertEquals(604800, getSecondsInUnit(Schedule.TimeUnit.WEEKS))
    Assertions.assertEquals(2592000, getSecondsInUnit(Schedule.TimeUnit.MONTHS))
  }

  // Will throw if a new TimeUnit is added but an appropriate mapping is not included in this method.
  @Test
  fun testAllOfTimeUnitEnumValues() {
    Arrays
      .stream(Schedule.TimeUnit.entries.toTypedArray())
      .forEach { obj: Schedule.TimeUnit -> getSecondsInUnit(obj) }
  }
}
