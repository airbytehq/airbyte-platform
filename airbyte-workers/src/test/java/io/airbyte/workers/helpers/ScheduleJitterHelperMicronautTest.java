/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test the ScheduleJitterHelper with values from application-schedule-jitter-test.yaml.
 */
@MicronautTest(environments = {"schedule-jitter-test"})
class ScheduleJitterHelperMicronautTest {

  @Inject
  @Value("${sanity-check}")
  String sanityCheck;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.no-jitter-cutoff-minutes}")
  int noJitterCutoffMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.threshold-minutes}")
  int highFrequencyThresholdMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.jitter-amount-minutes}")
  int highFrequencyJitterAmountMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.threshold-minutes}")
  int mediumFrequencyThresholdMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.jitter-amount-minutes}")
  int mediumFrequencyJitterAmountMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.threshold-minutes}")
  int lowFrequencyThresholdMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.jitter-amount-minutes}")
  int lowFrequencyJitterAmountMinutes;

  @Inject
  @Value("${airbyte.worker.connection.schedule-jitter.very-low-frequency-bucket.jitter-amount-minutes}")
  int veryLowFrequencyJitterAmountMinutes;

  @Inject
  ScheduleJitterHelper scheduleJitterHelper;

  @Test
  void verifyTestConfigIsLoaded() {
    assertEquals("jitter test", sanityCheck);
  }

  @Test
  void testNoJitterCutoffMinutes() {
    final Duration waitTime = Duration.ofMinutes(noJitterCutoffMinutes - 1);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    final Duration jitteredWaitTimeForBasic = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC);
    final Duration jitteredWaitTimeForCron = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON);

    assertEquals(waitTime, jitteredWaitTimeForBasic);
    assertEquals(waitTime, jitteredWaitTimeForCron);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testHighFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(highFrequencyThresholdMinutes - 1);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    final Duration jitteredWaitTimeForBasic = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC);
    final Duration jitteredWaitTimeForCron = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON);

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, highFrequencyJitterAmountMinutes, true);
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, highFrequencyJitterAmountMinutes, false);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testMediumFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(mediumFrequencyThresholdMinutes - 1);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    final Duration jitteredWaitTimeForBasic = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC);
    final Duration jitteredWaitTimeForCron = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON);

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, mediumFrequencyJitterAmountMinutes, true);
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, mediumFrequencyJitterAmountMinutes, false);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testLowFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(lowFrequencyThresholdMinutes - 1);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    final Duration jitteredWaitTimeForBasic = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC);
    final Duration jitteredWaitTimeForCron = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, lowFrequencyJitterAmountMinutes, true);
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, lowFrequencyJitterAmountMinutes, false);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testVeryLowFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(lowFrequencyThresholdMinutes + 1);

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    final Duration jitteredWaitTimeForBasic = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC);
    final Duration jitteredWaitTimeForCron = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON);

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, veryLowFrequencyJitterAmountMinutes, true);
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, veryLowFrequencyJitterAmountMinutes, false);
  }

  private void assertJitterBetween(final Duration originalWaitTime,
                                   final Duration jitteredWaitTime,
                                   final int jitterAmountMinutes,
                                   final boolean includeNegativeJitter) {

    final Duration minExpectedWaitTime;
    final Duration maxExpectedWaitTime;
    final int jitterAmountSeconds = jitterAmountMinutes * 60;

    if (includeNegativeJitter) {
      minExpectedWaitTime = originalWaitTime.minusSeconds(jitterAmountSeconds / 2);
      maxExpectedWaitTime = originalWaitTime.plusSeconds(jitterAmountSeconds / 2);
    } else {
      minExpectedWaitTime = originalWaitTime; // no negative jitter
      maxExpectedWaitTime = originalWaitTime.plusSeconds(jitterAmountSeconds);
    }

    assertTrue(jitteredWaitTime.compareTo(minExpectedWaitTime) >= 0);
    assertTrue(jitteredWaitTime.compareTo(maxExpectedWaitTime) <= 0);
  }

}
