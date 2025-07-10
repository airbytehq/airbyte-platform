/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.BasicSchedule
import io.airbyte.config.Schedule
import io.airbyte.config.StandardSync
import java.util.concurrent.TimeUnit

/**
 * Schedule helpers.
 */
object ScheduleHelpers {
  /**
   * Get number of seconds in time unit.
   *
   * @param timeUnitEnum time unit to get in seconds
   * @return seconds in time unit
   */
  @JvmStatic
  fun getSecondsInUnit(timeUnitEnum: Schedule.TimeUnit): Long =
    when (timeUnitEnum) {
      Schedule.TimeUnit.MINUTES -> TimeUnit.MINUTES.toSeconds(1)
      Schedule.TimeUnit.HOURS -> TimeUnit.HOURS.toSeconds(1)
      Schedule.TimeUnit.DAYS -> TimeUnit.DAYS.toSeconds(1)
      Schedule.TimeUnit.WEEKS -> TimeUnit.DAYS.toSeconds(1) * 7
      Schedule.TimeUnit.MONTHS -> TimeUnit.DAYS.toSeconds(1) * 30
      else -> throw RuntimeException("Unhandled TimeUnitEnum: $timeUnitEnum")
    }

  private fun getSecondsInUnit(timeUnitEnum: BasicSchedule.TimeUnit): Long =
    when (timeUnitEnum) {
      BasicSchedule.TimeUnit.MINUTES -> TimeUnit.MINUTES.toSeconds(1)
      BasicSchedule.TimeUnit.HOURS -> TimeUnit.HOURS.toSeconds(1)
      BasicSchedule.TimeUnit.DAYS -> TimeUnit.DAYS.toSeconds(1)
      BasicSchedule.TimeUnit.WEEKS -> TimeUnit.DAYS.toSeconds(1) * 7
      BasicSchedule.TimeUnit.MONTHS -> TimeUnit.DAYS.toSeconds(1) * 30
      else -> throw RuntimeException("Unhandled TimeUnitEnum: $timeUnitEnum")
    }

  @JvmStatic
  fun getIntervalInSecond(schedule: Schedule): Long = getSecondsInUnit(schedule.timeUnit) * schedule.units

  @JvmStatic
  fun getIntervalInSecond(schedule: BasicSchedule): Long = getSecondsInUnit(schedule.timeUnit) * schedule.units

  /**
   * Test if schedule configuration is consistent.
   *
   * @param standardSync sync to test
   * @return true if mismatch. otherwise, false.
   */
  @JvmStatic
  fun isScheduleTypeMismatch(standardSync: StandardSync): Boolean {
    if (standardSync.scheduleType == null) {
      return false
    }
    return (standardSync.manual && standardSync.scheduleType != StandardSync.ScheduleType.MANUAL) ||
      (
        !standardSync.manual &&
          standardSync.scheduleType == StandardSync.ScheduleType.MANUAL
      )
  }
}
