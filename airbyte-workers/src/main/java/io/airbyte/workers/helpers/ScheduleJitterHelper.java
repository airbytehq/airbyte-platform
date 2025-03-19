/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to compute and apply random jitter to scheduled connections.
 */
@Singleton
public class ScheduleJitterHelper {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
  public Duration addJitterBasedOnWaitTime(final Duration waitTime, final ConnectionScheduleType scheduleType) {
    // If the wait time is less than the cutoff, don't add any jitter.
    if (waitTime.toMinutes() <= noJitterCutoffMinutes) {
      log.debug("Wait time {} minutes was less than jitter cutoff of {} minutes. Not adding any jitter.", waitTime.toMinutes(),
          noJitterCutoffMinutes);
      return waitTime;
    }

    final int jitterSeconds;
    final Random random = new Random();

    // CRON schedules should not have negative jitter included, because then it is possible for the sync
    // to start and finish before the real scheduled time. This can result in a double sync because the
    // next computed wait time will be very short in this scenario.
    final Boolean includeNegativeJitter = !scheduleType.equals(ConnectionScheduleType.CRON);

    switch (determineFrequencyBucket(waitTime)) {
      case HIGH_FREQUENCY_BUCKET -> jitterSeconds = getRandomJitterSeconds(random, highFrequencyJitterAmountMinutes, includeNegativeJitter);
      case MEDIUM_FREQUENCY_BUCKET -> jitterSeconds = getRandomJitterSeconds(random, mediumFrequencyJitterAmountMinutes, includeNegativeJitter);
      case LOW_FREQUENCY_BUCKET -> jitterSeconds = getRandomJitterSeconds(random, lowFrequencyJitterAmountMinutes, includeNegativeJitter);
      case VERY_LOW_FREQUENCY_BUCKET -> jitterSeconds = getRandomJitterSeconds(random, veryLowFrequencyJitterAmountMinutes, includeNegativeJitter);
      default -> jitterSeconds = 0;
    }

    log.debug("Adding {} minutes of jitter to original wait duration of {} minutes", jitterSeconds / 60, waitTime.toMinutes());

    Duration newWaitTime = waitTime.plusSeconds(jitterSeconds);

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

  private static int getRandomJitterSeconds(final Random random, final int maximumJitterMinutes, final Boolean includeNegativeJitter) {
    // convert to seconds because fractional minutes are annoying to work with, and this guarantees an
    // even number of seconds so that
    // dividing by two later on doesn't result in a fractional number of seconds.
    final int maximumJitterSeconds = maximumJitterMinutes * 60;

    // random.nextInt is inclusive of 0 and exclusive of the provided value, so we add 1 to ensure
    // the maximum jitter is included in the possible range.
    int computedJitterSeconds = random.nextInt(maximumJitterSeconds + 1);

    if (includeNegativeJitter) {
      // if negative jitter is included, we need to shift the positive jitter to the left by half of the
      // maximum jitter.
      computedJitterSeconds -= maximumJitterSeconds / 2;
    }

    return computedJitterSeconds;
  }

}
