/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test the ScheduleJitterHelper with values from application-schedule-jitter-test.yaml
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
    final Duration jitteredWaitTime = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime);

    assertEquals(waitTime, jitteredWaitTime);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testHighFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(highFrequencyThresholdMinutes - 1);
    final Duration jitteredWaitTime = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime);

    assertJitterBetween(waitTime, jitteredWaitTime, highFrequencyJitterAmountMinutes);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testMediumFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(mediumFrequencyThresholdMinutes - 1);
    final Duration jitteredWaitTime = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime);

    assertJitterBetween(waitTime, jitteredWaitTime, mediumFrequencyJitterAmountMinutes);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testLowFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(lowFrequencyThresholdMinutes - 1);
    final Duration jitteredWaitTime = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime);

    assertJitterBetween(waitTime, jitteredWaitTime, lowFrequencyJitterAmountMinutes);
  }

  @RepeatedTest(50) // repeat a bunch because randomness is involved
  void testVeryLowFreqBucket() {
    final Duration waitTime = Duration.ofMinutes(lowFrequencyThresholdMinutes + 1);
    final Duration jitteredWaitTime = scheduleJitterHelper.addJitterBasedOnWaitTime(waitTime);

    assertJitterBetween(waitTime, jitteredWaitTime, veryLowFrequencyJitterAmountMinutes);
  }

  private void assertJitterBetween(final Duration originalWaitTime, final Duration jitteredWaitTime, final int jitterAmountMinutes) {
    // assert that the jittered wait time falls within the expected range
    final Duration minExpectedWaitTime = originalWaitTime.plusMinutes(jitterAmountMinutes * -1);
    final Duration maxExpectedWaitTime = originalWaitTime.plusMinutes(jitterAmountMinutes);
    assertTrue(jitteredWaitTime.compareTo(minExpectedWaitTime) >= 0);
    assertTrue(jitteredWaitTime.compareTo(maxExpectedWaitTime) <= 0);
  }

}
