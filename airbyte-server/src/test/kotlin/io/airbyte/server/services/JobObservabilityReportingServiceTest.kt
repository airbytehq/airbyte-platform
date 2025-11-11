/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
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

    // Parse the evaluation JSON to verify all fields are present
    val evaluationJson = result[evaluationKey] as String
    val evaluationNode: JsonNode = Jsons.deserialize(evaluationJson)

    // Verify top-level evaluation fields
    assertEquals(3.5, evaluationNode.get("value").asDouble(), 0.001, "Evaluation should contain value (z-score)")
    assertEquals(3.0, evaluationNode.get("threshold").asDouble(), 0.001, "Evaluation should contain threshold")
    assertTrue(evaluationNode.get("is_outlier").asBoolean(), "Evaluation should contain is_outlier=true")

    // Verify scores is a proper JSON object (not string) and contains all fields
    val scoresNode = evaluationNode.get("scores")
    assertNotNull(scoresNode, "Scores should be present")
    assertTrue(scoresNode.isObject, "Scores should be a JSON object, not a string")

    // Verify scores object contains the expected fields with proper values
    assertEquals(50.0, scoresNode.get("current").asDouble(), 0.001, "Scores should include current value")
    assertEquals(9.09, scoresNode.get("mean").asDouble(), 0.1, "Scores should include mean")
    assertEquals(11.69, scoresNode.get("std").asDouble(), 0.1, "Scores should include standard deviation")

    // zScore could be serialized as either "zScore" or "zscore" depending on Jackson config
    val zScoreValue = scoresNode.get("zScore")?.asDouble() ?: scoresNode.get("zscore")?.asDouble() ?: 0.0
    assertNotNull(scoresNode.get("zScore") ?: scoresNode.get("zscore"), "zScore field should exist in JSON")
    assertEquals(3.5, zScoreValue, 0.1, "Scores should include z-score")
  }

  @Test
  fun `buildStreamSummary serializes scores as JSON objects not strings`() {
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

    // Parse the serialized JSON
    val scoreJson = result["_score_bytesLoaded"] as String
    val scoreNode: JsonNode = Jsons.deserialize(scoreJson)

    // Verify top-level fields
    assertEquals(5.007937940360045, scoreNode.get("value").asDouble(), 0.0001)
    assertEquals(4.0, scoreNode.get("threshold").asDouble(), 0.0001)
    assertTrue(scoreNode.get("is_outlier").asBoolean())

    // Verify scores is a proper JSON object, not a string
    val scoresNode = scoreNode.get("scores")
    assertNotNull(scoresNode)
    assertTrue(scoresNode.isObject, "scores should be a JSON object, not a string")

    // Verify scores object contains the expected fields with proper types
    // Note: Field names in JSON match the Kotlin property names exactly
    assertEquals(88696.0, scoresNode.get("current").asDouble(), 0.0001)
    assertEquals(26632.321167883212, scoresNode.get("mean").asDouble(), 0.0001)
    assertEquals(12393.069691094492, scoresNode.get("std").asDouble(), 0.0001)

    // zScore could be serialized as either "zScore" or "zscore" depending on Jackson config
    val zScoreValue = scoresNode.get("zScore")?.asDouble() ?: scoresNode.get("zscore")?.asDouble() ?: 0.0
    assertNotNull(scoresNode.get("zScore") ?: scoresNode.get("zscore"), "zScore field should exist in JSON")
    assertEquals(5.007937940360045, zScoreValue, 0.0001)
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

    // Parse the serialized JSON
    val scoreJson = result["_score_nulledValuePerRecord"] as String
    val scoreNode: JsonNode = Jsons.deserialize(scoreJson)

    // Verify scores is null
    val scoresNode = scoreNode.get("scores")
    assertTrue(scoresNode.isNull, "scores should be null when OutlierEvaluation.scores is null")
  }
}
