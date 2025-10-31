/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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
}
