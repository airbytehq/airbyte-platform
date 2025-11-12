/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.statistics.OutlierEvaluation
import io.airbyte.statistics.Scores
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class JobObservabilityReportingServiceTest {
  private val service = JobObservabilityReportingService(sentryHub = null)

  @Test
  fun `buildStreamSummary includes additionalStats when present`() {
    val streamInfo =
      StreamInfo(
        namespace = "public",
        name = "users",
        wasBackfilled = false,
        wasResumed = false,
        metrics =
          StreamMetrics(
            bytesLoaded = 1000L,
            recordsLoaded = 100L,
            recordsRejected = 5L,
            additionalStats = mapOf("wal_size" to BigDecimal("100.0"), "replication_lag" to BigDecimal("5.0")),
          ),
        evaluations = emptyList(),
        isOutlier = false,
      )

    // Call method directly
    val result = service.buildStreamSummary(streamInfo)

    // Verify standard fields are present
    assertEquals(1000L, result["bytes_loaded"])
    assertEquals(100L, result["records_loaded"])
    assertEquals(5L, result["records_rejected"])
    assertEquals(false, result["was_backfilled"])
    assertEquals(false, result["was_resumed"])

    // Verify additionalStats is included
    assertTrue(result.containsKey("additional_stats"))
    @Suppress("UNCHECKED_CAST")
    val additionalStats = result["additional_stats"] as Map<String, BigDecimal>
    assertEquals(BigDecimal("100.0"), additionalStats["wal_size"])
    assertEquals(BigDecimal("5.0"), additionalStats["replication_lag"])
  }

  @Test
  fun `buildStreamSummary does not include additionalStats when empty`() {
    val streamInfo =
      StreamInfo(
        namespace = "public",
        name = "users",
        wasBackfilled = false,
        wasResumed = false,
        metrics =
          StreamMetrics(
            bytesLoaded = 1000L,
            recordsLoaded = 100L,
            recordsRejected = 5L,
            additionalStats = emptyMap(),
          ),
        evaluations = emptyList(),
        isOutlier = false,
      )

    // Call method directly
    val result = service.buildStreamSummary(streamInfo)

    // Verify standard fields are present
    assertEquals(1000L, result["bytes_loaded"])
    assertEquals(100L, result["records_loaded"])

    // Verify additionalStats is NOT included when empty
    assertFalse(result.containsKey("additional_stats"))
  }

  @Test
  fun `buildStreamSummary includes derived metrics in additionalStats and evaluations`() {
    // Create evaluation for derived metric with full scores
    val derivedMetricEvaluation =
      OutlierEvaluation(
        name = "sourceFieldsPopulatedPerRecord",
        value = 3.5, // z-score
        threshold = 3.0,
        isOutlier = true,
        scores =
          Scores(
            current = 50.0,
            mean = 9.09,
            std = 11.69,
            zScore = 3.5,
          ),
      )

    val streamInfo =
      StreamInfo(
        namespace = "public",
        name = "users",
        wasBackfilled = false,
        wasResumed = false,
        metrics =
          StreamMetrics(
            bytesLoaded = 1000L,
            recordsLoaded = 100L,
            recordsRejected = 0L,
            additionalStats =
              mapOf(
                "sourceFieldsPopulated" to BigDecimal("5000.0"), // raw metric
                "sourceFieldsPopulatedPerRecord" to BigDecimal("50.0"), // derived metric
              ),
          ),
        evaluations = listOf(derivedMetricEvaluation),
        isOutlier = true,
      )

    // Call method directly
    val result = service.buildStreamSummary(streamInfo)

    // ========== Verify standard fields ==========
    assertEquals(1000L, result["bytes_loaded"])
    assertEquals(100L, result["records_loaded"])
    assertEquals(0L, result["records_rejected"])
    assertEquals(false, result["was_backfilled"])
    assertEquals(false, result["was_resumed"])

    // ========== Verify additionalStats includes derived metrics ==========
    assertTrue(result.containsKey("additional_stats"), "Should include additional_stats")
    @Suppress("UNCHECKED_CAST")
    val additionalStats = result["additional_stats"] as Map<String, BigDecimal>

    // Verify raw metric is included
    assertTrue(additionalStats.containsKey("sourceFieldsPopulated"), "Should include raw metric")
    assertEquals(BigDecimal("5000.0"), additionalStats["sourceFieldsPopulated"])

    // Verify derived metric is included
    assertTrue(additionalStats.containsKey("sourceFieldsPopulatedPerRecord"), "Should include derived metric")
    assertEquals(BigDecimal("50.0"), additionalStats["sourceFieldsPopulatedPerRecord"])

    // ========== Verify evaluation for derived metric is included ==========
    val evaluationKey = "_score_sourceFieldsPopulatedPerRecord"
    assertTrue(result.containsKey(evaluationKey), "Should include evaluation for derived metric with key $evaluationKey")

    // Get the evaluation map directly (no longer a JSON string)
    @Suppress("UNCHECKED_CAST")
    val evaluationMap = result[evaluationKey] as Map<String, Any?>

    // Verify top-level evaluation fields
    assertEquals(3.5, (evaluationMap["value"] as Number).toDouble(), 0.001, "Evaluation should contain value (z-score)")
    assertEquals(3.0, (evaluationMap["threshold"] as Number).toDouble(), 0.001, "Evaluation should contain threshold")
    assertTrue(evaluationMap["is_outlier"] as Boolean, "Evaluation should contain is_outlier=true")

    // Verify scores is present and is a map (not a string)
    assertNotNull(evaluationMap["scores"], "Scores should be present")
    @Suppress("UNCHECKED_CAST")
    val scoresMap = evaluationMap["scores"] as Map<String, Any?>

    // Verify scores object contains the expected fields with proper values
    assertEquals(50.0, (scoresMap["current"] as Number).toDouble(), 0.001, "Scores should include current value")
    assertEquals(9.09, (scoresMap["mean"] as Number).toDouble(), 0.1, "Scores should include mean")
    assertEquals(11.69, (scoresMap["std"] as Number).toDouble(), 0.1, "Scores should include standard deviation")
    assertEquals(3.5, (scoresMap["zScore"] as Number).toDouble(), 0.1, "Scores should include z-score")
  }

  @Test
  fun `buildStreamSummary includes scores as map objects not strings`() {
    val scores =
      Scores(
        current = 88696.0,
        mean = 26632.321167883212,
        std = 12393.069691094492,
        zScore = 5.007937940360045,
      )

    val evaluation =
      OutlierEvaluation(
        name = "bytesLoaded",
        value = 5.007937940360045,
        threshold = 4.0,
        isOutlier = true,
        scores = scores,
      )

    val streamInfo =
      StreamInfo(
        namespace = "public",
        name = "users",
        wasBackfilled = false,
        wasResumed = false,
        metrics =
          StreamMetrics(
            bytesLoaded = 88696L,
            recordsLoaded = 400L,
            recordsRejected = 0L,
            additionalStats = emptyMap(),
          ),
        evaluations = listOf(evaluation),
        isOutlier = true,
      )

    val result = service.buildStreamSummary(streamInfo)

    // Verify the _score_ field exists
    assertTrue(result.containsKey("_score_bytesLoaded"))

    // Get the score map directly (no longer a JSON string)
    @Suppress("UNCHECKED_CAST")
    val scoreMap = result["_score_bytesLoaded"] as Map<String, Any?>

    // Verify top-level fields
    assertEquals(5.007937940360045, (scoreMap["value"] as Number).toDouble(), 0.0001)
    assertEquals(4.0, (scoreMap["threshold"] as Number).toDouble(), 0.0001)
    assertTrue(scoreMap["is_outlier"] as Boolean)

    // Verify scores is a proper map object, not a string
    assertNotNull(scoreMap["scores"])
    @Suppress("UNCHECKED_CAST")
    val scoresMap = scoreMap["scores"] as Map<String, Any?>

    // Verify scores object contains the expected fields with proper types
    assertEquals(88696.0, (scoresMap["current"] as Number).toDouble(), 0.0001)
    assertEquals(26632.321167883212, (scoresMap["mean"] as Number).toDouble(), 0.0001)
    assertEquals(12393.069691094492, (scoresMap["std"] as Number).toDouble(), 0.0001)
    assertEquals(5.007937940360045, (scoresMap["zScore"] as Number).toDouble(), 0.0001)
  }

  @Test
  fun `buildStreamSummary handles null scores correctly`() {
    val evaluation =
      OutlierEvaluation(
        name = "nulledValuePerRecord",
        value = Double.NaN,
        threshold = 3.0,
        isOutlier = false,
        scores = null,
      )

    val streamInfo =
      StreamInfo(
        namespace = "public",
        name = "users",
        wasBackfilled = false,
        wasResumed = false,
        metrics =
          StreamMetrics(
            bytesLoaded = 1000L,
            recordsLoaded = 100L,
            recordsRejected = 0L,
            additionalStats = emptyMap(),
          ),
        evaluations = listOf(evaluation),
        isOutlier = false,
      )

    val result = service.buildStreamSummary(streamInfo)

    // Verify the _score_ field exists
    assertTrue(result.containsKey("_score_nulledValuePerRecord"))

    // Get the score map directly (no longer a JSON string)
    @Suppress("UNCHECKED_CAST")
    val scoreMap = result["_score_nulledValuePerRecord"] as Map<String, Any?>

    // Verify scores is null
    assertEquals(null, scoreMap["scores"], "scores should be null when OutlierEvaluation.scores is null")
  }
}
