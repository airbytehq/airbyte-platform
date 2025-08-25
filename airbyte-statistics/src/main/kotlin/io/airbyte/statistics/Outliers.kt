/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

import org.jetbrains.kotlinx.dataframe.api.last
import org.jetbrains.kotlinx.dataframe.api.mean
import org.jetbrains.kotlinx.dataframe.api.std
import org.jetbrains.kotlinx.dataframe.api.toDataFrame

/**
 * Output of the outlier scoring.
 */
data class Scores(
  val current: Double,
  val mean: Double,
  val std: Double,
  val zScore: Double,
)

class Outliers {
  /**
   * Scoring phase for outlier detection.
   *
   * getScores will take an object of type [T], convert it to a dataframe and evaluate z-scores on any numerical fields of [T]
   */
  inline fun <reified T> getScores(
    historicalData: List<T>,
    current: T,
  ): Map<String, Scores> {
    val df = (historicalData + current).toDataFrame()
    val mean = df.mean()
    val std = df.std()
    val c = df.last()

    return mean.df().columns().associate {
      val name = it.name()
      name to
        Scores(
          current = c[name].toDouble(),
          mean = mean[name].toDouble(),
          std = std[name].toDouble(),
          zScore = zScore(c[name].toDouble(), mean[name].toDouble(), std[name].toDouble()),
        )
    }
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
