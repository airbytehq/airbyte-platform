/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper to compute and apply random jitter to scheduled connections.
 */
@Singleton
@Slf4j
public class ScheduleJitterHelper {

  private final int noJitterCutoffMinutes;
  private final int highFrequencyThresholdMinutes;
  private final int highFrequencyJitterAmountMinutes;
  private final int mediumFrequencyThresholdMinutes;
  private final int mediumFrequencyJitterAmountMinutes;
  private final int lowFrequencyThresholdMinutes;
  private final int lowFrequencyJitterAmountMinutes;
  private final int veryLowFrequencyJitterAmountMinutes;

  @SuppressWarnings("LineLength")
  public ScheduleJitterHelper(
                              @Value("${airbyte.worker.connection.schedule-jitter.no-jitter-cutoff-minutes}") final int noJitterCutoffMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.threshold-minutes}") final int highFrequencyThresholdMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.jitter-amount-minutes}") final int highFrequencyJitterAmountMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.threshold-minutes}") final int mediumFrequencyThresholdMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.jitter-amount-minutes}") final int mediumFrequencyJitterAmountMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.threshold-minutes}") final int lowFrequencyThresholdMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.jitter-amount-minutes}") final int lowFrequencyJitterAmountMinutes,
                              @Value("${airbyte.worker.connection.schedule-jitter.very-low-frequency-bucket.jitter-amount-minutes}") final int veryLowFrequencyJitterAmountMinutes) {
    this.noJitterCutoffMinutes = noJitterCutoffMinutes;
    this.highFrequencyThresholdMinutes = highFrequencyThresholdMinutes;
    this.highFrequencyJitterAmountMinutes = highFrequencyJitterAmountMinutes;
    this.mediumFrequencyThresholdMinutes = mediumFrequencyThresholdMinutes;
    this.mediumFrequencyJitterAmountMinutes = mediumFrequencyJitterAmountMinutes;
    this.lowFrequencyThresholdMinutes = lowFrequencyThresholdMinutes;
    this.lowFrequencyJitterAmountMinutes = lowFrequencyJitterAmountMinutes;
    this.veryLowFrequencyJitterAmountMinutes = veryLowFrequencyJitterAmountMinutes;
  }

  /**
   * Defines which frequency bucket a connection's wait time falls into. Then, based on the bucket,
   * adds a random amount of jitter to the wait time. For instance, connections that run often fall
   * into the high frequency bucket and will have less jitter applied than connections that run less
   * often.
   */
  public Duration addJitterBasedOnWaitTime(final Duration waitTime) {
    // If the wait time is less than the cutoff, don't add any jitter.
    if (waitTime.toMinutes() <= noJitterCutoffMinutes) {
      log.debug("Wait time {} minutes was less than jitter cutoff of {} minutes. Not adding any jitter.", waitTime.toMinutes(),
          noJitterCutoffMinutes);
      return waitTime;
    }

    final int jitterMinutes;
    final Random random = new Random();

    switch (determineFrequencyBucket(waitTime)) {
      case HIGH_FREQUENCY_BUCKET -> jitterMinutes = getRandomJitter(random, highFrequencyJitterAmountMinutes);
      case MEDIUM_FREQUENCY_BUCKET -> jitterMinutes = getRandomJitter(random, mediumFrequencyJitterAmountMinutes);
      case LOW_FREQUENCY_BUCKET -> jitterMinutes = getRandomJitter(random, lowFrequencyJitterAmountMinutes);
      case VERY_LOW_FREQUENCY_BUCKET -> jitterMinutes = getRandomJitter(random, veryLowFrequencyJitterAmountMinutes);
      default -> jitterMinutes = 0;
    }

    log.debug("Adding {} minutes of jitter to original wait duration of {} minutes", jitterMinutes, waitTime.toMinutes());

    Duration newWaitTime = waitTime.plusMinutes(jitterMinutes);

    // If the jitter results in a negative wait time, set it to 0 seconds to keep things sane.
    if (newWaitTime.isNegative()) {
      log.debug("Jitter resulted in a negative wait time of {}. Setting wait time to 0 seconds.", newWaitTime);
      newWaitTime = Duration.ZERO;
    }

    return newWaitTime;
  }

  private enum FrequencyBucket {
    HIGH_FREQUENCY_BUCKET,
    MEDIUM_FREQUENCY_BUCKET,
    LOW_FREQUENCY_BUCKET,
    VERY_LOW_FREQUENCY_BUCKET
  }

  private FrequencyBucket determineFrequencyBucket(final Duration waitTime) {
    final long waitMinutes = waitTime.toMinutes();

    if (waitMinutes <= this.highFrequencyThresholdMinutes) {
      return FrequencyBucket.HIGH_FREQUENCY_BUCKET;
    } else if (waitMinutes <= this.mediumFrequencyThresholdMinutes) {
      return FrequencyBucket.MEDIUM_FREQUENCY_BUCKET;
    } else if (waitMinutes <= this.lowFrequencyThresholdMinutes) {
      return FrequencyBucket.LOW_FREQUENCY_BUCKET;
    } else {
      return FrequencyBucket.VERY_LOW_FREQUENCY_BUCKET;
    }
  }

  private static int getRandomJitter(final Random random, final int maxMinutesToAdd) {
    // return a random value between 0 and maxMinutesToAdd inclusive (hence the + 1)
    return random.nextInt(maxMinutesToAdd + 1);
  }

}
