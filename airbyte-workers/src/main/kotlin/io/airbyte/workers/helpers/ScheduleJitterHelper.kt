/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.Random

/**
 * Helper to compute and apply random jitter to scheduled connections.
 */
@Singleton
class ScheduleJitterHelper(
  @param:Value("\${airbyte.worker.connection.schedule-jitter.no-jitter-cutoff-minutes}") private val noJitterCutoffMinutes: Int,
  @param:Value("\${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.threshold-minutes}") private val highFrequencyThresholdMinutes:
    Int,
  @param:Value(
    "\${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.jitter-amount-minutes}",
  ) private val highFrequencyJitterAmountMinutes: Int,
  @param:Value(
    "\${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.threshold-minutes}",
  ) private val mediumFrequencyThresholdMinutes: Int,
  @param:Value(
    "\${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.jitter-amount-minutes}",
  ) private val mediumFrequencyJitterAmountMinutes: Int,
  @param:Value("\${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.threshold-minutes}") private val lowFrequencyThresholdMinutes: Int,
  @param:Value(
    "\${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.jitter-amount-minutes}",
  ) private val lowFrequencyJitterAmountMinutes: Int,
  @param:Value(
    "\${airbyte.worker.connection.schedule-jitter.very-low-frequency-bucket.jitter-amount-minutes}",
  ) private val veryLowFrequencyJitterAmountMinutes: Int,
) {
  /**
   * Defines which frequency bucket a connection's wait time falls into. Then, based on the bucket,
   * adds a random amount of jitter to the wait time. For instance, connections that run often fall
   * into the high frequency bucket and will have less jitter applied than connections that run less
   * often.
   */
  fun addJitterBasedOnWaitTime(
    waitTime: Duration,
    scheduleType: ConnectionScheduleType?,
  ): Duration {
    // If the wait time is less than the cutoff, don't add any jitter.
    if (waitTime.toMinutes() <= noJitterCutoffMinutes) {
      log.debug(
        "Wait time {} minutes was less than jitter cutoff of {} minutes. Not adding any jitter.",
        waitTime.toMinutes(),
        noJitterCutoffMinutes,
      )
      return waitTime
    }

    val jitterSeconds: Int
    val random = Random()

    // CRON schedules should not have negative jitter included, because then it is possible for the sync
    // to start and finish before the real scheduled time. This can result in a double sync because the
    // next computed wait time will be very short in this scenario.
    val includeNegativeJitter = scheduleType != ConnectionScheduleType.CRON

    jitterSeconds =
      when (determineFrequencyBucket(waitTime)) {
        FrequencyBucket.HIGH_FREQUENCY_BUCKET ->
          getRandomJitterSeconds(
            random,
            highFrequencyJitterAmountMinutes,
            includeNegativeJitter,
          )

        FrequencyBucket.MEDIUM_FREQUENCY_BUCKET ->
          getRandomJitterSeconds(
            random,
            mediumFrequencyJitterAmountMinutes,
            includeNegativeJitter,
          )

        FrequencyBucket.LOW_FREQUENCY_BUCKET ->
          getRandomJitterSeconds(
            random,
            lowFrequencyJitterAmountMinutes,
            includeNegativeJitter,
          )

        FrequencyBucket.VERY_LOW_FREQUENCY_BUCKET ->
          getRandomJitterSeconds(
            random,
            veryLowFrequencyJitterAmountMinutes,
            includeNegativeJitter,
          )

        else -> 0
      }

    log.debug("Adding {} minutes of jitter to original wait duration of {} minutes", jitterSeconds / 60, waitTime.toMinutes())

    var newWaitTime = waitTime.plusSeconds(jitterSeconds.toLong())

    // If the jitter results in a negative wait time, set it to 0 seconds to keep things sane.
    if (newWaitTime.isNegative) {
      log.debug("Jitter resulted in a negative wait time of {}. Setting wait time to 0 seconds.", newWaitTime)
      newWaitTime = Duration.ZERO
    }

    return newWaitTime
  }

  private enum class FrequencyBucket {
    HIGH_FREQUENCY_BUCKET,
    MEDIUM_FREQUENCY_BUCKET,
    LOW_FREQUENCY_BUCKET,
    VERY_LOW_FREQUENCY_BUCKET,
  }

  private fun determineFrequencyBucket(waitTime: Duration): FrequencyBucket {
    val waitMinutes = waitTime.toMinutes()

    return when {
      waitMinutes <= this.highFrequencyThresholdMinutes -> {
        FrequencyBucket.HIGH_FREQUENCY_BUCKET
      }
      waitMinutes <= this.mediumFrequencyThresholdMinutes -> {
        FrequencyBucket.MEDIUM_FREQUENCY_BUCKET
      }
      waitMinutes <= this.lowFrequencyThresholdMinutes -> {
        FrequencyBucket.LOW_FREQUENCY_BUCKET
      }
      else -> {
        FrequencyBucket.VERY_LOW_FREQUENCY_BUCKET
      }
    }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private fun getRandomJitterSeconds(
      random: Random,
      maximumJitterMinutes: Int,
      includeNegativeJitter: Boolean,
    ): Int {
      // convert to seconds because fractional minutes are annoying to work with, and this guarantees an
      // even number of seconds so that
      // dividing by two later on doesn't result in a fractional number of seconds.
      val maximumJitterSeconds = maximumJitterMinutes * 60

      // random.nextInt is inclusive of 0 and exclusive of the provided value, so we add 1 to ensure
      // the maximum jitter is included in the possible range.
      var computedJitterSeconds = random.nextInt(maximumJitterSeconds + 1)

      if (includeNegativeJitter) {
        // if negative jitter is included, we need to shift the positive jitter to the left by half of the
        // maximum jitter.
        computedJitterSeconds -= maximumJitterSeconds / 2
      }

      return computedJitterSeconds
    }
  }
}
