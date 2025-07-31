/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.JobRead
import jakarta.annotation.Nullable
import org.quartz.CronExpression
import java.time.Duration
import java.util.Date
import java.util.function.Supplier
import kotlin.math.max

/**
 * Static helper class for cron scheduling logic.
 */
object CronSchedulingHelper {
  const val MS_PER_SECOND: Long = 1000L
  private const val MIN_CRON_INTERVAL_SECONDS: Long = 60

  @JvmStatic
  fun getNextRuntimeBasedOnPreviousJobAndSchedule(
    currentSecondsSupplier: Supplier<Long>,
    @Nullable priorJobRead: JobRead?,
    cronExpression: CronExpression,
  ): Duration {
    // get the earliest possible next run based on the prior job's start time.
    val earliestNextRun = getEarliestNextRun(currentSecondsSupplier, priorJobRead)

    // determine the next cron run according to the earliest possible start time.
    val nextRunStartDate = cronExpression.getNextValidTimeAfter(earliestNextRun)

    // calculate the number of seconds between now and the next cron run.
    // this can be negative if the next cron run should have already started.
    val nextRunStartSeconds = nextRunStartDate.time / MS_PER_SECOND - currentSecondsSupplier.get()

    // max with 0 so that we never return a negative value.
    return Duration.ofSeconds(max(0.0, nextRunStartSeconds.toDouble()).toLong())
  }

  /**
   * Ensure that at least a minimum interval -- one minute -- passes between executions. This prevents
   * us from multiple executions for the same scheduled time, since cron only has a 1-minute
   * resolution.
   */
  private fun getEarliestNextRun(
    currentSecondsSupplier: Supplier<Long>,
    @Nullable priorJobRead: JobRead?,
  ): Date {
    val earliestNextRunSeconds = if (priorJobRead == null) currentSecondsSupplier.get() else priorJobRead.createdAt + MIN_CRON_INTERVAL_SECONDS
    return Date(earliestNextRunSeconds * MS_PER_SECOND)
  }
}
