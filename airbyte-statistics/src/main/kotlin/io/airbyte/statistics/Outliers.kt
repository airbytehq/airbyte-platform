/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

import org.jetbrains.kotlinx.dataframe.api.last
import org.jetbrains.kotlinx.dataframe.api.mean
import org.jetbrains.kotlinx.dataframe.api.std
import org.jetbrains.kotlinx.dataframe.api.toDataFrame

/** Output of the outlier detection.
 *
 * isOutlier is the decision, other fields are for debugging purpose.
 */
data class Scores(
  val mean: MutableMap<String, Double> = mutableMapOf(),
  val std: MutableMap<String, Double> = mutableMapOf(),
  val scores: MutableMap<String, Double> = mutableMapOf(),
)

class Outliers {
  /**
   * Outlier detection using z-scores.
   *
   * Evaluate will take an object of type [T], convert it to a dataframe and evaluate z-scores on any numerical fields of [T]
   */
  inline fun <reified T> evaluate(
    historicalData: List<T>,
    current: T,
  ): Scores {
    val df = (historicalData + current).toDataFrame()
    val mean = df.mean()
    val std = df.std()
    val c = df.last()

    val scores = Scores()
    mean.df().columns().forEach {
      scores.mean[it.name()] = mean[it.name()].toDouble()
      scores.std[it.name()] = std[it.name()].toDouble()
      scores.scores[it.name()] = zScore(c[it.name()].toDouble(), mean[it.name()].toDouble(), std[it.name()].toDouble())
    }
    return scores
  }

  /**
   * compute z-score
   */
  fun zScore(
    x: Double,
    mean: Double,
    std: Double,
  ): Double = (x - mean) / std

  // Because mean() and std() returns a dataframe of Doubles, however, the whole thing isn't typed.
  fun Any?.toDouble(): Double =
    when (this) {
      is Int -> this.toDouble()
      is Long -> this.toDouble()
      is Float -> this.toDouble()
      is Double -> this
      else -> TODO(this?.javaClass?.name ?: "null")
    }
}
