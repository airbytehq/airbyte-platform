/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.JobRead
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.quartz.CronExpression
import java.text.ParseException
import java.time.Duration
import java.util.Date
import java.util.function.Supplier

internal class CronSchedulingHelperTest {
  private lateinit var currentSecondsSupplier: Supplier<Long>

  @BeforeEach
  fun setup() {
    this.currentSecondsSupplier = Mockito.mock(Supplier::class.java) as Supplier<Long>
  }

  @Test
  fun testNextRunWhenPriorJobIsNull() {
    // set current time to 3 hours before the next run
    val nextRun: Date = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW)
    val threeHoursBeforeNextRun: Long = nextRun.time / CronSchedulingHelper.MS_PER_SECOND - Duration.ofHours(3).seconds
    Mockito.`when`(currentSecondsSupplier.get()).thenReturn(threeHoursBeforeNextRun)

    val actualNextRuntimeSeconds =
      CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, null, EVERY_DAY_AT_MIDNIGHT).seconds

    // Expect duration to be exactly three hours
    Assertions.assertEquals(Duration.ofHours(3).seconds, actualNextRuntimeSeconds)
  }

  @Test
  fun testNextRunWhenPriorJobStartedLongTimeAgo() {
    // set current time to 5 minutes after a scheduled run, to simulate waiting for jitter to kick in.
    val nextRun: Date = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW)
    val fiveMinutesAfterRun: Long = nextRun.time / CronSchedulingHelper.MS_PER_SECOND + Duration.ofMinutes(5).seconds
    Mockito.`when`(currentSecondsSupplier.get()).thenReturn(fiveMinutesAfterRun)

    // set prior job createdAt to 10 minutes after it's previous run schedule, to simulate jitter having
    // delayed the previous run slightly.
    val tenMinutesAfterPreviousRun: Long =
      nextRun.time / CronSchedulingHelper.MS_PER_SECOND - Duration.ofHours(24).seconds + Duration.ofMinutes(10).seconds
    val priorJobRead = Mockito.mock(JobRead::class.java)
    Mockito.`when`(priorJobRead.createdAt).thenReturn(tenMinutesAfterPreviousRun)

    val actualNextRuntimeSeconds =
      CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, priorJobRead, EVERY_DAY_AT_MIDNIGHT).seconds

    // Expect duration to be 0, because the next run should start immediately.
    Assertions.assertEquals(0, actualNextRuntimeSeconds)
  }

  @Test
  fun testNextRunWhenPriorJobStartedRecently() {
    // set current time to 20 minutes after a scheduled run, this time we'll simulate the job started
    // recently.
    val nextRun: Date = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW)
    val twentyMinutesAfterRun: Long = nextRun.time / CronSchedulingHelper.MS_PER_SECOND + Duration.ofMinutes(20).seconds
    Mockito.`when`(currentSecondsSupplier.get()).thenReturn(twentyMinutesAfterRun)

    // set prior job createdAt to 5 minutes ago.
    val fiveMinutesAgo = twentyMinutesAfterRun - Duration.ofMinutes(5).seconds
    val priorJobRead = Mockito.mock(JobRead::class.java)
    Mockito.`when`(priorJobRead.createdAt).thenReturn(fiveMinutesAgo)

    val actualNextRuntimeSeconds =
      CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, priorJobRead, EVERY_DAY_AT_MIDNIGHT).seconds

    // Since we know a job started 5 minutes ago, and it is 20 minutes after the scheduled run, we would
    // expect to wait 23 hours and 40 minutes
    // for the next run.
    val expectedWaitTimeSeconds = Duration.ofHours(23).seconds + Duration.ofMinutes(40).seconds

    Assertions.assertEquals(expectedWaitTimeSeconds, actualNextRuntimeSeconds)
  }

  companion object {
    // fix the current time so that tests are deterministic
    private const val CURRENT_EPOCH_SECONDS: Long = 1698366192
    private val NOW: Date = Date(CURRENT_EPOCH_SECONDS * 1000L)

    private val EVERY_DAY_AT_MIDNIGHT: CronExpression

    init {
      try {
        EVERY_DAY_AT_MIDNIGHT = CronExpression("0 0 0 * * ? *")
      } catch (e: ParseException) {
        throw RuntimeException(e)
      }
    }
  }
}
