/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.BasicSchedule;
import io.airbyte.config.Schedule;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import java.util.concurrent.TimeUnit;

/**
 * Schedule helpers.
 */
@SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
public class ScheduleHelpers {

  /**
   * Get number of seconds in time unit.
   *
   * @param timeUnitEnum time unit to get in seconds
   * @return seconds in time unit
   */
  public static Long getSecondsInUnit(final Schedule.TimeUnit timeUnitEnum) {
    switch (timeUnitEnum) {
      case MINUTES:
        return TimeUnit.MINUTES.toSeconds(1);
      case HOURS:
        return TimeUnit.HOURS.toSeconds(1);
      case DAYS:
        return TimeUnit.DAYS.toSeconds(1);
      case WEEKS:
        return TimeUnit.DAYS.toSeconds(1) * 7;
      case MONTHS:
        return TimeUnit.DAYS.toSeconds(1) * 30;
      default:
        throw new RuntimeException("Unhandled TimeUnitEnum: " + timeUnitEnum);
    }
  }

  private static Long getSecondsInUnit(final BasicSchedule.TimeUnit timeUnitEnum) {
    switch (timeUnitEnum) {
      case MINUTES:
        return TimeUnit.MINUTES.toSeconds(1);
      case HOURS:
        return TimeUnit.HOURS.toSeconds(1);
      case DAYS:
        return TimeUnit.DAYS.toSeconds(1);
      case WEEKS:
        return TimeUnit.DAYS.toSeconds(1) * 7;
      case MONTHS:
        return TimeUnit.DAYS.toSeconds(1) * 30;
      default:
        throw new RuntimeException("Unhandled TimeUnitEnum: " + timeUnitEnum);
    }
  }

  public static Long getIntervalInSecond(final Schedule schedule) {
    return getSecondsInUnit(schedule.getTimeUnit()) * schedule.getUnits();
  }

  public static Long getIntervalInSecond(final BasicSchedule schedule) {
    return getSecondsInUnit(schedule.getTimeUnit()) * schedule.getUnits();
  }

  /**
   * Test if schedule configuration is consistent.
   *
   * @param standardSync sync to test
   * @return true if mismatch. otherwise, false.
   */
  public static boolean isScheduleTypeMismatch(final StandardSync standardSync) {
    if (standardSync.getScheduleType() == null) {
      return false;
    }
    return (standardSync.getManual() && standardSync.getScheduleType() != ScheduleType.MANUAL) || (!standardSync.getManual()
        && standardSync.getScheduleType() == ScheduleType.MANUAL);
  }

}
