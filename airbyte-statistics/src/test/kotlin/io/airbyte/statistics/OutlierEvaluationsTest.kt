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
