/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.micronaut.context.annotation.Property
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
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
internal class ScheduleJitterHelperMicronautTest {
  @Inject
  lateinit var airbyteWorkerConfig: AirbyteWorkerConfig

  @Inject
  @Value("\${sanity-check}")
  var sanityCheck: String? = null

  @Inject
  var scheduleJitterHelper: ScheduleJitterHelper? = null

  @Test
  fun verifyTestConfigIsLoaded() {
    Assertions.assertEquals("jitter test", sanityCheck)
  }

  @Test
  fun testNoJitterCutoffMinutes() {
    val waitTime = Duration.ofMinutes((airbyteWorkerConfig.connection.scheduleJitter.noJitterCutoffMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    Assertions.assertEquals(waitTime, jitteredWaitTimeForBasic)
    Assertions.assertEquals(waitTime, jitteredWaitTimeForCron)
  }

  @RepeatedTest(50)
  fun testHighFreqBucket() {
    val waitTime = Duration.ofMinutes((airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.thresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForBasic,
      airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.jitterAmountMinutes,
      true,
    )
    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForCron,
      airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.jitterAmountMinutes,
      false,
    )
  }

  @RepeatedTest(50)
  fun testMediumFreqBucket() {
    val waitTime = Duration.ofMinutes((airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.thresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForBasic,
      airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.jitterAmountMinutes,
      true,
    )
    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForCron,
      airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.jitterAmountMinutes,
      false,
    )
  }

  @RepeatedTest(50)
  fun testLowFreqBucket() {
    val waitTime = Duration.ofMinutes((airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.thresholdMinutes - 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForBasic,
      airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.jitterAmountMinutes,
      true,
    )
    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForCron,
      airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.jitterAmountMinutes,
      false,
    )
  }

  @RepeatedTest(50)
  fun testVeryLowFreqBucket() {
    val waitTime = Duration.ofMinutes((airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.thresholdMinutes + 1).toLong())

    // normally would use @ParameterizedTest for this, but it can't be combined with @RepeatedTest
    val jitteredWaitTimeForBasic = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.BASIC)
    val jitteredWaitTimeForCron = scheduleJitterHelper!!.addJitterBasedOnWaitTime(waitTime, ConnectionScheduleType.CRON)

    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForBasic,
      airbyteWorkerConfig.connection.scheduleJitter.veryLowFrequencyBucket.jitterAmountMinutes,
      true,
    )
    assertJitterBetween(
      waitTime,
      jitteredWaitTimeForCron,
      airbyteWorkerConfig.connection.scheduleJitter.veryLowFrequencyBucket.jitterAmountMinutes,
      false,
    )
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
