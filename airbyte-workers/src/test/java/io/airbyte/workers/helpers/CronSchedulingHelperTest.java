/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static io.airbyte.workers.helpers.CronSchedulingHelper.MS_PER_SECOND;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.model.generated.JobRead;
import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.CronExpression;

class CronSchedulingHelperTest {

  // fix the current time so that tests are deterministic
  private static final long CURRENT_EPOCH_SECONDS = 1698366192;
  private static final Date NOW = new Date(CURRENT_EPOCH_SECONDS * 1000L);

  private static final CronExpression EVERY_DAY_AT_MIDNIGHT;

  static {
    try {
      EVERY_DAY_AT_MIDNIGHT = new CronExpression("0 0 0 * * ? *");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private Supplier<Long> currentSecondsSupplier;

  @BeforeEach
  void setup() {
    this.currentSecondsSupplier = mock(Supplier.class);
  }

  @Test
  void testNextRunWhenPriorJobIsNull() {
    // set current time to 3 hours before the next run
    final Date nextRun = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW);
    final long threeHoursBeforeNextRun = nextRun.getTime() / MS_PER_SECOND - Duration.ofHours(3).toSeconds();
    when(currentSecondsSupplier.get()).thenReturn(threeHoursBeforeNextRun);

    final long actualNextRuntimeSeconds =
        CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, null, EVERY_DAY_AT_MIDNIGHT).getSeconds();

    // Expect duration to be exactly three hours
    Assertions.assertEquals(Duration.ofHours(3).toSeconds(), actualNextRuntimeSeconds);
  }

  @Test
  void testNextRunWhenPriorJobStartedLongTimeAgo() {
    // set current time to 5 minutes after a scheduled run, to simulate waiting for jitter to kick in.
    final Date nextRun = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW);
    final long fiveMinutesAfterRun = nextRun.getTime() / MS_PER_SECOND + Duration.ofMinutes(5).toSeconds();
    when(currentSecondsSupplier.get()).thenReturn(fiveMinutesAfterRun);

    // set prior job createdAt to 10 minutes after it's previous run schedule, to simulate jitter having
    // delayed the previous run slightly.
    final long tenMinutesAfterPreviousRun = nextRun.getTime() / MS_PER_SECOND - Duration.ofHours(24).toSeconds() + Duration.ofMinutes(10).toSeconds();
    final JobRead priorJobRead = mock(JobRead.class);
    when(priorJobRead.getCreatedAt()).thenReturn(tenMinutesAfterPreviousRun);

    final long actualNextRuntimeSeconds =
        CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, priorJobRead, EVERY_DAY_AT_MIDNIGHT).getSeconds();

    // Expect duration to be 0, because the next run should start immediately.
    Assertions.assertEquals(0, actualNextRuntimeSeconds);
  }

  @Test
  void testNextRunWhenPriorJobStartedRecently() {
    // set current time to 20 minutes after a scheduled run, this time we'll simulate the job started
    // recently.
    final Date nextRun = EVERY_DAY_AT_MIDNIGHT.getNextValidTimeAfter(NOW);
    final long twentyMinutesAfterRun = nextRun.getTime() / MS_PER_SECOND + Duration.ofMinutes(20).toSeconds();
    when(currentSecondsSupplier.get()).thenReturn(twentyMinutesAfterRun);

    // set prior job createdAt to 5 minutes ago.
    final long fiveMinutesAgo = twentyMinutesAfterRun - Duration.ofMinutes(5).toSeconds();
    final JobRead priorJobRead = mock(JobRead.class);
    when(priorJobRead.getCreatedAt()).thenReturn(fiveMinutesAgo);

    final long actualNextRuntimeSeconds =
        CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, priorJobRead, EVERY_DAY_AT_MIDNIGHT).getSeconds();

    // Since we know a job started 5 minutes ago, and it is 20 minutes after the scheduled run, we would
    // expect to wait 23 hours and 40 minutes
    // for the next run.
    final long expectedWaitTimeSeconds = Duration.ofHours(23).toSeconds() + Duration.ofMinutes(40).toSeconds();

    Assertions.assertEquals(expectedWaitTimeSeconds, actualNextRuntimeSeconds);
  }

}
