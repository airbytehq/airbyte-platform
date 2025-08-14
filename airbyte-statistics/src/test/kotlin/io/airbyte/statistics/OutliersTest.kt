/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

import io.airbyte.statistics.OutliersTest.Fixtures.defaultStreamStats
import io.airbyte.statistics.OutliersTest.Fixtures.isOutlier
import io.airbyte.statistics.OutliersTest.Fixtures.randomize
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.random.nextLong
import kotlin.time.Duration.Companion.minutes

private val logger = KotlinLogging.logger {}

data class StreamStats(
  val streamName: String,
  val duration: Long,
  val attempts: Int,
  val bytesLoaded: Long,
  val recordsLoaded: Long,
  val recordsRejected: Long,
)

class OutliersTest {
  @Test
  fun `naive outlier verification`() {
    val historical =
      listOf(
        defaultStreamStats.copy(),
        defaultStreamStats.copy(),
        defaultStreamStats.copy(),
        defaultStreamStats.copy(),
        defaultStreamStats.copy(),
        defaultStreamStats.copy(),
      )

    val current = defaultStreamStats.copy(recordsLoaded = 3)
    logger.info { historical }
    logger.info { current }

    val outcome = Outliers().evaluate(historical, current)
    logger.info { "outcome: ${outcome.isOutlier()} $outcome" }

    assertTrue(outcome.isOutlier())
  }

  @Test
  fun `in range outlier verification with randomized data`() {
    val historical =
      listOf(
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
      )
    val current = defaultStreamStats.randomize(range = 0.1)
    logger.info { historical }
    logger.info { current }

    val outcome = Outliers().evaluate(historical, current)
    logger.info { "outcome: ${outcome.isOutlier()} $outcome" }

    assertFalse(outcome.isOutlier())
  }

  @Test
  fun `out of range outlier verification with randomized data`() {
    val historical =
      listOf(
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
        defaultStreamStats.randomize(range = 0.1),
      )
    val current = defaultStreamStats.randomize(range = 1.0)
    logger.info { historical }
    logger.info { current }

    val outcome = Outliers().evaluate(historical, current)
    logger.info { "outcome: ${outcome.isOutlier()} $outcome" }

    assertTrue(outcome.isOutlier())
  }

  object Fixtures {
    val defaultStreamStats =
      StreamStats(
        streamName = "stream1",
        duration = 10.minutes.inWholeSeconds,
        attempts = 0,
        bytesLoaded = 10000,
        recordsLoaded = 100,
        recordsRejected = 0,
      )

    fun StreamStats.randomize(range: Double): StreamStats =
      copy(
        duration = randomize(duration, range),
        attempts = randomize(attempts, range),
        bytesLoaded = randomize(bytesLoaded, range),
        recordsLoaded = randomize(recordsLoaded, range),
        recordsRejected = randomize(recordsRejected, range),
      )

    fun randomize(
      value: Long,
      range: Double,
    ): Long = Random.nextLong(((1.0 - range) * value).toLong()..((1.0 + range) * value).toLong())

    fun randomize(
      value: Int,
      range: Double,
    ): Int = Random.nextInt(((1.0 - range) * value).toInt()..((1.0 + range) * value).toInt())

    fun Scores.isOutlier() = scores.any { it.value.absoluteValue > 2.0 }
  }
}
