/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

import kotlin.math.absoluteValue
import kotlin.math.sqrt

/**
 * The Result of an outlier evaluation.
 */
data class OutlierEvaluation(
  val name: String,
  val value: Double,
  val threshold: Double,
  val isOutlier: Boolean,
  val scores: Scores?,
)

/**
 * Define a rule to determine outliers.
 *
 * @param name the name of the rule.
 * @param value the value to evaluate.
 * @param operator the operator to use to compare the evaluated value to the threshold.
 * @param threshold the threshold to use to compare the evaluated value to.
 */
data class OutlierRule(
  val name: String,
  val value: Expression,
  val operator: ComparisonOperator,
  val threshold: Expression,
) {
  fun evaluate(sc: ScoringContext): OutlierEvaluation? {
    val v = value.getValue(sc)
    val t = threshold.getValue(sc)
    val scores = value.getScores(sc)
    return if (v != null && t != null) {
      OutlierEvaluation(
        name = name,
        value = v,
        threshold = t,
        isOutlier = operator.compare(v, t),
        scores = scores,
      )
    } else {
      null
    }
  }
}

/**
 * Comparison operators used for OutlierRules.
 */
sealed interface ComparisonOperator {
  fun compare(
    lhs: Double,
    rhs: Double,
  ): Boolean
}

object GreaterThan : ComparisonOperator {
  override fun compare(
    lhs: Double,
    rhs: Double,
  ): Boolean = lhs > rhs
}

object LessThan : ComparisonOperator {
  override fun compare(
    lhs: Double,
    rhs: Double,
  ): Boolean = lhs < rhs
}

/**
 * Convenience aliasing
 */
typealias ScoringContext = Map<String, Scores>

/**
 * Base interface of an expression tree.
 *
 * We use this to represent the Dimension lookup as well as the arithmetic operations required on the value before it can be evaluated.
 */
sealed interface Expression {
  /**
   * Returns the value of the expression.
   */
  fun getValue(sc: ScoringContext): Double?

  /**
   * Returns the most relevant scores for the expression. Typically, for debugging the scoring.
   */
  fun getScores(sc: ScoringContext): Scores? = null
}

/**
 * A Constant expression.
 */
data class Const(
  val value: Double,
) : Expression {
  override fun getValue(sc: ScoringContext): Double = value
}

/**
 * Represent a dimension.
 *
 * A dimension refers to a dimension of the [ScoringContext] by name.
 * Those dimensions have pre-computed statistical values associated with them.
 *
 * @param name the name of the dimension.
 * @param f the statistical function to read to the dimension.
 */
data class Dimension(
  val name: String,
  val f: StatisticalFunction = StatisticalFunction.IDENTITY,
) : Expression {
  enum class StatisticalFunction {
    IDENTITY,
    MEAN,
    STD,
    ZSCORE,
  }

  override fun getValue(sc: Map<String, Scores>): Double? =
    sc[name]?.let { scores ->
      when (f) {
        StatisticalFunction.IDENTITY -> scores.current
        StatisticalFunction.MEAN -> scores.mean
        StatisticalFunction.STD -> scores.std
        StatisticalFunction.ZSCORE -> scores.zScore
      }
    }

  override fun getScores(sc: Map<String, Scores>): Scores? = sc[name]
}

/**
 * Defines an operator to combine two expressions.
 */
data class Operator(
  val lhs: Expression,
  val rhs: Expression,
  val type: Type,
) : Expression {
  enum class Type {
    Divide,
    Minus,
    Plus,
    Times,
  }

  override fun getValue(sc: Map<String, Scores>): Double? {
    val lhsValue = lhs.getValue(sc) ?: return null
    val rhsValue = rhs.getValue(sc) ?: return null
    return when (type) {
      Type.Divide -> lhsValue / rhsValue
      Type.Minus -> lhsValue - rhsValue
      Type.Plus -> lhsValue + rhsValue
      Type.Times -> lhsValue * rhsValue
    }
  }
}

sealed interface Function : Expression {
  val value: Expression

  fun apply(value: Double): Double

  override fun getValue(sc: ScoringContext): Double? = value.getValue(sc)?.let { apply(it) }

  override fun getScores(sc: ScoringContext): Scores? = value.getScores(sc)
}

/**
 * Returns the absolute value of the expression.
 */
data class Abs(
  override val value: Expression,
) : Function {
  override fun apply(value: Double): Double = value.absoluteValue
}

/**
 * 1+(1/x)
 *
 * Returns a coefficient to adjust the outlier threshold. The intent is to increase the thresholds for connections that
 * generally run faster.
 * The desired function has a high enough value for f(0) and should converge towards 1 as x goes towards infinity
 */
data class Reciprocal(
  override val value: Expression,
) : Function {
  override fun getValue(sc: ScoringContext): Double {
    // Ensures we always provide a value, also put a floor on it to avoid arbitrarily high thresholds.
    // f(0.01) being 101
    val value = value.getValue(sc)
    return apply(if (value == null || value < 0.01) 0.01 else value)
  }

  override fun apply(value: Double): Double = 1.0 + (1.0 / value)
}

/**
 * 1+(1/sqrt(x))
 *
 * Returns a coefficient to adjust the outlier threshold. The intent is to increase the thresholds for connections with
 *  a lower volume of data.
 * The desired function has a high enough value for f(0) and should converge towards 1 as x goes towards infinity
 */
data class ReciprocalSqrt(
  override val value: Expression,
) : Function {
  override fun getValue(sc: ScoringContext): Double {
    // Ensures we always provide a value, also put a floor on it to avoid arbitrarily high thresholds.
    // f(0.01) being 11
    val value = value.getValue(sc)
    return apply(if (value == null || value < 0.01) 0.01 else value)
  }

  override fun apply(value: Double): Double = 1.0 + (1.0 / sqrt(value))
}
