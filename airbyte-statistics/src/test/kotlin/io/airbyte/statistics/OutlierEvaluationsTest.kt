/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull

class OutlierEvaluationsTest {
  @Test
  fun `comparison operator GreaterThan compare as expected`() {
    assertFalse { GreaterThan.compare(1.0, 1.1) }
    assertTrue { GreaterThan.compare(1.2, 1.1) }
    assertFalse { GreaterThan.compare(1.3, 1.3) }
  }

  @Test
  fun `comparison operator LessThan compare as expected`() {
    assertTrue { LessThan.compare(1.0, 1.1) }
    assertFalse { LessThan.compare(1.2, 1.1) }
    assertFalse { LessThan.compare(1.3, 1.3) }
  }

  @Test
  fun `Const getValue returns the given value`() {
    assertEquals(1.5, Const(1.5).getValue(defaultScoringContext))
  }

  @Test
  fun `Const returns no scores`() {
    assertNull(Const(1.6).getScores(defaultScoringContext))
  }

  @Test
  fun `Dimension looks up the correct values`() {
    assertEquals(DEFAULT_CURRENT, DURATION_DIM.identity.getValue(defaultScoringContext))
    assertEquals(DEFAULT_MEAN, DURATION_DIM.mean.getValue(defaultScoringContext))
    assertEquals(DEFAULT_STD, DURATION_DIM.std.getValue(defaultScoringContext))
    assertEquals(DEFAULT_ZSCORE, DURATION_DIM.zScore.getValue(defaultScoringContext))

    assertEquals(DURATION_SCORES, DURATION_DIM.getScores(defaultScoringContext))
  }

  @Test
  fun `Dimension returns nulls for unknown dimensions`() {
    val unknownDim = Dimension("unknown")

    assertNull(unknownDim.identity.getValue(defaultScoringContext))
    assertNull(unknownDim.mean.getValue(defaultScoringContext))
    assertNull(unknownDim.std.getValue(defaultScoringContext))
    assertNull(unknownDim.zScore.getValue(defaultScoringContext))
    assertNull(unknownDim.getScores(defaultScoringContext))
  }

  @Test
  fun `Operations do the expected math`() {
    assertEquals(DEFAULT_CURRENT + DEFAULT_MEAN, (DURATION_DIM.identity + DURATION_DIM.mean).getValue(defaultScoringContext))
    assertEquals(DEFAULT_CURRENT - DEFAULT_MEAN, (DURATION_DIM.identity - DURATION_DIM.mean).getValue(defaultScoringContext))
    assertEquals(DEFAULT_CURRENT * DEFAULT_MEAN, (DURATION_DIM.identity * DURATION_DIM.mean).getValue(defaultScoringContext))
    assertEquals(DEFAULT_CURRENT / DEFAULT_MEAN, (DURATION_DIM.identity / DURATION_DIM.mean).getValue(defaultScoringContext))
  }

  @Test
  fun `function abs returns the absolute value`() {
    assertEquals(42.0, Abs(Const(-42.0)).getValue(defaultScoringContext))
    assertEquals(51.0, Abs(Const(51.0)).getValue(defaultScoringContext))

    // Note that apply does the abs value of the parameter and ignores the expression provided in the constructor
    assertEquals(27.0, Abs(Const(-42.0)).apply(-27.0))
  }

  @Test
  fun `function reciprocal returns the reciprocal`() {
    assertEquals(1.25, Reciprocal(Const(4.0)).getValue(defaultScoringContext))
    assertEquals(1.5, Reciprocal(Const(2.0)).getValue(defaultScoringContext))
    assertEquals(101.0, Reciprocal(Const(0.0)).getValue(defaultScoringContext))
  }

  @Test
  fun `function reciprocalSqrt returns the reciprocal of the square root`() {
    assertEquals(1.25, ReciprocalSqrt(Const(16.0)).getValue(defaultScoringContext))
    assertEquals(1.2, ReciprocalSqrt(Const(25.0)).getValue(defaultScoringContext))
    assertEquals(11.0, ReciprocalSqrt(Const(0.0)).getValue(defaultScoringContext))
  }

  @Test
  fun `function return the scores of their underlying expression`() {
    assertEquals(DURATION_SCORES, Abs(DURATION_DIM).getScores(defaultScoringContext))
    assertEquals(DURATION_SCORES, Reciprocal(DURATION_DIM).getScores(defaultScoringContext))
    assertEquals(DURATION_SCORES, ReciprocalSqrt(DURATION_DIM).getScores(defaultScoringContext))
  }

  @Test
  fun `OutlierRule evaluations return the expected summary`() {
    val ruleName = "myRule"
    val rule =
      OutlierRule(
        name = ruleName,
        value = DURATION_DIM.identity,
        operator = LessThan,
        threshold = Const(6.0),
      )
    val outlierEval = rule.evaluate(defaultScoringContext)

    val expected =
      OutlierEvaluation(
        name = ruleName,
        value = DEFAULT_CURRENT,
        threshold = 6.0,
        isOutlier = true,
        scores = DURATION_SCORES,
      )
    assertEquals(expected, outlierEval)
  }

  @Test
  fun `OutlierRule evaluations returns null for unknown dimensions`() {
    val ruleName = "myRule"
    val rule =
      OutlierRule(
        name = ruleName,
        value = DURATION_DIM.identity,
        operator = LessThan,
        threshold = Const(6.0),
      )
    val outlierEval = rule.evaluate(emptyMap())
    assertNull(outlierEval)
  }

  @Test
  fun `DerivedStatRule computes simple division`() {
    val rule =
      DerivedStatRule(
        name = "bytesPerRecord",
        value = Dimension("bytesLoaded") / Dimension("recordsLoaded"),
      )

    val context =
      mapOf(
        "bytesLoaded" to Scores(current = 1000.0, mean = 0.0, std = 0.0, zScore = 0.0),
        "recordsLoaded" to Scores(current = 10.0, mean = 0.0, std = 0.0, zScore = 0.0),
      )

    val result = rule.compute(context)
    assertEquals(100.0, result?.toDouble())
  }

  @Test
  fun `DerivedStatRule returns null when dimension is missing`() {
    val rule =
      DerivedStatRule(
        name = "bytesPerRecord",
        value = Dimension("bytesLoaded") / Dimension("recordsLoaded"),
      )

    val context =
      mapOf(
        "bytesLoaded" to Scores(current = 1000.0, mean = 0.0, std = 0.0, zScore = 0.0),
        // recordsLoaded is missing
      )

    val result = rule.compute(context)
    assertNull(result)
  }

  @Test
  fun `DerivedStatRule handles division by zero`() {
    val rule =
      DerivedStatRule(
        name = "bytesPerRecord",
        value = Dimension("bytesLoaded") / Dimension("recordsLoaded"),
      )

    val context =
      mapOf(
        "bytesLoaded" to Scores(current = 1000.0, mean = 0.0, std = 0.0, zScore = 0.0),
        "recordsLoaded" to Scores(current = 0.0, mean = 0.0, std = 0.0, zScore = 0.0),
      )

    val result = rule.compute(context)
    // Division by zero results in null (infinity is not a valid BigDecimal)
    assertNull(result)
  }

  @Test
  fun `DerivedStatRule can reference custom dimensions`() {
    val rule =
      DerivedStatRule(
        name = "recordsPerApiCall",
        value = Dimension("recordsLoaded") / Dimension("api_calls"),
      )

    val context =
      mapOf(
        "recordsLoaded" to Scores(current = 100.0, mean = 0.0, std = 0.0, zScore = 0.0),
        "api_calls" to Scores(current = 5.0, mean = 0.0, std = 0.0, zScore = 0.0),
      )

    val result = rule.compute(context)
    assertEquals(20.0, result?.toDouble())
  }

  @Test
  fun `DerivedStatRule supports complex expressions`() {
    val rule =
      DerivedStatRule(
        name = "rejectionRatePercent",
        value =
          (
            Dimension("recordsRejected") /
              (Dimension("recordsLoaded") + Dimension("recordsRejected"))
          ) * Const(100.0),
      )

    val context =
      mapOf(
        "recordsLoaded" to Scores(current = 90.0, mean = 0.0, std = 0.0, zScore = 0.0),
        "recordsRejected" to Scores(current = 10.0, mean = 0.0, std = 0.0, zScore = 0.0),
      )

    val result = rule.compute(context)
    assert(result != null)
    assertEquals(10.0, result!!.toDouble(), 0.001)
  }

  @Test
  fun `OutlierRule uses debugScores when provided`() {
    // Create a rule with a complex expression that wouldn't naturally return scores
    // but provide debugScores explicitly
    val rule =
      OutlierRule(
        name = "complexRule",
        value = (DURATION_DIM.identity - DURATION_DIM.mean) / (DURATION_DIM.mean * Const(0.02)),
        operator = GreaterThan,
        threshold = Const(3.0),
        debugScores = DURATION_DIM,
      )
    val outlierEval = rule.evaluate(defaultScoringContext)

    // Should use the debugScores dimension for scores
    assertEquals(DURATION_SCORES, outlierEval?.scores)
  }

  @Test
  fun `OutlierRule falls back to value getScores when debugScores is null`() {
    // Create a rule without debugScores - should infer from value expression
    val rule =
      OutlierRule(
        name = "simpleRule",
        value = DURATION_DIM.zScore,
        operator = GreaterThan,
        threshold = Const(3.0),
        // debugScores is null (default)
      )
    val outlierEval = rule.evaluate(defaultScoringContext)

    // Should infer scores from the value expression
    assertEquals(DURATION_SCORES, outlierEval?.scores)
  }

  @Test
  fun `OutlierRule returns null scores when debugScores is null and value has no scores`() {
    // Create a rule with a complex expression and no debugScores
    val rule =
      OutlierRule(
        name = "noScoresRule",
        value = Const(10.0) + Const(20.0),
        operator = GreaterThan,
        threshold = Const(3.0),
        // debugScores is null, and value expression has no scores
      )
    val outlierEval = rule.evaluate(defaultScoringContext)

    // Should have null scores
    assertNull(outlierEval?.scores)
  }

  companion object {
    val DURATION_DIM = Dimension("duration")
    val DURATION_SCORES = Scores(DEFAULT_CURRENT, DEFAULT_MEAN, DEFAULT_STD, DEFAULT_ZSCORE)
    const val DEFAULT_CURRENT = 5.0
    const val DEFAULT_MEAN = 6.0
    const val DEFAULT_STD = 7.0
    const val DEFAULT_ZSCORE = 2.0
    val defaultScoringContext =
      mapOf(
        "duration" to DURATION_SCORES,
        "other" to Scores(4.04, 4.04, 4.04, 4.04),
      )
  }
}
