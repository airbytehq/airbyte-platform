/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.model.generated.JobRead;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Date;
import java.util.function.Supplier;
import org.quartz.CronExpression;

/**
 * Static helper class for cron scheduling logic.
 */
public class CronSchedulingHelper {

  protected static final long MS_PER_SECOND = 1000L;
  private static final long MIN_CRON_INTERVAL_SECONDS = 60;

  public static Duration getNextRuntimeBasedOnPreviousJobAndSchedule(final Supplier<Long> currentSecondsSupplier,
                                                                     final @Nullable JobRead priorJobRead,
                                                                     final CronExpression cronExpression) {
    // get the earliest possible next run based on the prior job's start time.
    final Date earliestNextRun = getEarliestNextRun(currentSecondsSupplier, priorJobRead);

    // determine the next cron run according to the earliest possible start time.
    final Date nextRunStartDate = cronExpression.getNextValidTimeAfter(earliestNextRun);

    // calculate the number of seconds between now and the next cron run.
    // this can be negative if the next cron run should have already started.
    final long nextRunStartSeconds = nextRunStartDate.getTime() / MS_PER_SECOND - currentSecondsSupplier.get();

    // max with 0 so that we never return a negative value.
    return Duration.ofSeconds(Math.max(0, nextRunStartSeconds));
  }

  /**
   * Ensure that at least a minimum interval -- one minute -- passes between executions. This prevents
   * us from multiple executions for the same scheduled time, since cron only has a 1-minute
   * resolution.
   */
  private static Date getEarliestNextRun(final Supplier<Long> currentSecondsSupplier, final @Nullable JobRead priorJobRead) {
    final Long earliestNextRunSeconds = priorJobRead == null ? currentSecondsSupplier.get() : priorJobRead.getCreatedAt() + MIN_CRON_INTERVAL_SECONDS;
    return new Date(earliestNextRunSeconds * MS_PER_SECOND);
  }

}
