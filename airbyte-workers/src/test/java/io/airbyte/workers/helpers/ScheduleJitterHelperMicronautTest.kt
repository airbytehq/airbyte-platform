/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Test the ScheduleJitterHelper with values from application-schedule-jitter-test.yaml.
 */
@MicronautTest(environments = ["schedule-jitter-test"])
internal class ScheduleJitterHelperMicronautTest {
  @Inject
  @Value("\${sanity-check}")
  var sanityCheck: String? = null

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.no-jitter-cutoff-minutes}")
  var noJitterCutoffMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.threshold-minutes}")
  var highFrequencyThresholdMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.high-frequency-bucket.jitter-amount-minutes}")
  var highFrequencyJitterAmountMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.threshold-minutes}")
  var mediumFrequencyThresholdMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.medium-frequency-bucket.jitter-amount-minutes}")
  var mediumFrequencyJitterAmountMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.threshold-minutes}")
  var lowFrequencyThresholdMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.low-frequency-bucket.jitter-amount-minutes}")
  var lowFrequencyJitterAmountMinutes: Int = 0

  @Inject
  @Value("\${airbyte.worker.connection.schedule-jitter.very-low-frequency-bucket.jitter-amount-minutes}")
  var veryLowFrequencyJitterAmountMinutes: Int = 0

  @Inject
  var scheduleJitterHelper: ScheduleJitterHelper? = null

  @Test
  fun verifyTestConfigIsLoaded() {
    Assertions.assertEquals("jitter test", sanityCheck)
  }

  @Test
  fun testNoJitterCutoffMinutes() {
    val waitTime = Duration.ofMinutes((noJitterCutoffMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    Assertions.assertEquals(waitTime, jitteredWaitTimeForBasic)
    Assertions.assertEquals(waitTime, jitteredWaitTimeForCron)
  }

  @RepeatedTest(50)
  fun testHighFreqBucket() {
    val waitTime = Duration.ofMinutes((highFrequencyThresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, highFrequencyJitterAmountMinutes, true)
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, highFrequencyJitterAmountMinutes, false)
  }

  @RepeatedTest(50)
  fun testMediumFreqBucket() {
    val waitTime = Duration.ofMinutes((mediumFrequencyThresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, mediumFrequencyJitterAmountMinutes, true)
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, mediumFrequencyJitterAmountMinutes, false)
  }

  @RepeatedTest(50)
  fun testLowFreqBucket() {
    val waitTime = Duration.ofMinutes((lowFrequencyThresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, lowFrequencyJitterAmountMinutes, true)
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, lowFrequencyJitterAmountMinutes, false)
  }

  @RepeatedTest(50)
  fun testVeryLowFreqBucket() {
    val waitTime = Duration.ofMinutes((lowFrequencyThresholdMinutes + 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(waitTime, jitteredWaitTimeForBasic, veryLowFrequencyJitterAmountMinutes, true)
    assertJitterBetween(waitTime, jitteredWaitTimeForCron, veryLowFrequencyJitterAmountMinutes, false)
  }

  private fun assertJitterBetween(
    originalWaitTime: Duration,
    jitteredWaitTime: Duration,
    jitterAmountMinutes: Int,
    includeNegativeJitter: Boolean,
  ) {
    val minExpectedWaitTime: Duration
    val maxExpectedWaitTime: Duration
    val jitterAmountSeconds = jitterAmountMinutes * 60

    if (includeNegativeJitter) {
      minExpectedWaitTime = originalWaitTime.minusSeconds((jitterAmountSeconds / 2).toLong())
      maxExpectedWaitTime = originalWaitTime.plusSeconds((jitterAmountSeconds / 2).toLong())
    } else {
      minExpectedWaitTime = originalWaitTime // no negative jitter
      maxExpectedWaitTime = originalWaitTime.plusSeconds(jitterAmountSeconds.toLong())
    }

    Assertions.assertTrue(jitteredWaitTime.compareTo(minExpectedWaitTime) >= 0)
    Assertions.assertTrue(jitteredWaitTime.compareTo(maxExpectedWaitTime) <= 0)
  }
}
