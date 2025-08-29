/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.statistics.Abs
import io.airbyte.statistics.Const
import io.airbyte.statistics.Dimension
import io.airbyte.statistics.GreaterThan
import io.airbyte.statistics.OutlierRule
import io.airbyte.statistics.Reciprocal
import io.airbyte.statistics.ReciprocalSqrt
import io.airbyte.statistics.div
import io.airbyte.statistics.mean
import io.airbyte.statistics.plus
import io.airbyte.statistics.times
import io.airbyte.statistics.zScore
import jakarta.inject.Singleton

/**
 * The Job metrics to consider for outlier detection.
 */
data class JobMetrics(
  val attemptCount: Int,
  val durationSeconds: Long,
)

/**
 * The Stream metrics to consider for outlier detection.
 */
data class StreamMetrics(
  var bytesLoaded: Long,
  var recordsLoaded: Long,
  var recordsRejected: Long,
)

/**
 * Provides the rules used for outlier detection.
 *
 * The rules are currently hard-coded; however, this would be a good entrypoint if we wanted to make them configurable.
 */
@Singleton
class JobObservabilityRulesService {
  fun getJobOutlierRules() = jobOutlierRules

  fun getStreamOutlierRules() = streamOutlierRules

  /**
   * The dimensions that can be used for outlier detection.
   *
   * These dimensions need to match the fields of the [JobMetrics] and [StreamMetrics] classes.
   */
  object Dim {
    object Job {
      val attemptCount = Dimension("attemptCount")
      val durationSeconds = Dimension("durationSeconds")
    }

    object Stream {
      val bytesLoaded = Dimension("bytesLoaded")
      val recordsLoaded = Dimension("recordsLoaded")
      val recordsRejected = Dimension("recordsRejected")
    }
  }

  private val jobOutlierRules =
    listOf(
      OutlierRule(
        name = Dim.Job.durationSeconds.name,
        value = Dim.Job.durationSeconds.zScore,
        operator = GreaterThan,
        // Get the time coefficient from the duration in minutes for a more adequate drop-off.
        threshold = Const(3.0) * Reciprocal(Dim.Job.durationSeconds.mean / Const(60.0)),
      ),
      OutlierRule(
        name = Dim.Job.attemptCount.name,
        value = Dim.Job.attemptCount,
        operator = GreaterThan,
        // We arbitrarily flag syncs that have 3 more attempts than their average.
        threshold = Dim.Job.attemptCount.mean + Const(3.0),
      ),
    )

  /**
   * Standard deviation threshold for stream metrics when evaluating a z-score.
   *
   * 4.0 feels high; however, we are evaluating data from very different sources and usages. Our goal is to flag bigger variations that would
   * generally be associated with broken connector changes rather than subtle variation in data usage trends.
   */
  private val dataStdDevThreshold = Const(4.0)

  private val streamOutlierRules =
    listOf(
      OutlierRule(
        name = Dim.Stream.bytesLoaded.name,
        value = Abs(Dim.Stream.bytesLoaded.zScore),
        operator = GreaterThan,
        // We are adjusting the threshold for loaded data based on the average record count as a proxy for volume. Rationale being that a stream
        // moving a little amount of data will be more susceptible to variations.
        threshold = dataStdDevThreshold * ReciprocalSqrt(Dim.Stream.recordsLoaded.mean),
      ),
      OutlierRule(
        name = Dim.Stream.recordsLoaded.name,
        value = Abs(Dim.Stream.recordsLoaded.zScore),
        operator = GreaterThan,
        // We are adjusting the threshold for loaded data based on the average record count as a proxy for volume. Rationale being that a stream
        // moving a little amount of data will be more susceptible to variations.
        threshold = dataStdDevThreshold * ReciprocalSqrt(Dim.Stream.recordsLoaded.mean),
      ),
      OutlierRule(
        name = Dim.Stream.recordsRejected.name,
        value = Abs(Dim.Stream.recordsRejected.zScore),
        operator = GreaterThan,
        // RejectedRecords have their own deviation, mostly because they are more isolated events and shouldn't be tied to the same trend as the
        // "positive" amount of data loaded.
        threshold = dataStdDevThreshold * ReciprocalSqrt(Dim.Stream.recordsRejected.mean),
      ),
    )
}
