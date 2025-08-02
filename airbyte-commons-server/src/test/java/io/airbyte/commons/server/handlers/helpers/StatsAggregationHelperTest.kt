/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.getAggregatedStats
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.wasBackfilled
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Optional

internal class StatsAggregationHelperTest {
  @Test
  fun testAggregatedStatsFullRefresh() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.FULL_REFRESH,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_2,
        ),
      )

    Assertions.assertEquals(400L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(300L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(200L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(100L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(10L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testAggregatedStatsResumedFullRefresh() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.FULL_REFRESH,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_4_RESUMED,
          STREAM_SYNC_STATS_5_RESUMED,
        ),
      )

    Assertions.assertEquals(440040L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(330030L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(220020L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(110010L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(11001L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testAggregatedStatsResumedFullRefreshEdgeCaseWithNonResumedStatsMixed() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.FULL_REFRESH,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_2,
          STREAM_SYNC_STATS_4_RESUMED,
          STREAM_SYNC_STATS_5_RESUMED,
        ),
      )

    Assertions.assertEquals(440400L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(330300L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(220200L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(110100L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(11010L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testAggregatedStatsResumedFullRefreshEdgeCaseWithNonResumedStatsMixedEndingWithNonResumed() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.FULL_REFRESH,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_4_RESUMED,
          STREAM_SYNC_STATS_5_RESUMED,
          STREAM_SYNC_STATS_2,
        ),
      )

    Assertions.assertEquals(400L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(300L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(200L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(100L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(10L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testIncremental() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.INCREMENTAL,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_2,
        ),
      )

    Assertions.assertEquals(440L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(330L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(220L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(110L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(11L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testIncrementalIgnoresWasResumed() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.INCREMENTAL,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_2,
          STREAM_SYNC_STATS_4_RESUMED,
          STREAM_SYNC_STATS_5_RESUMED,
        ),
      )

    Assertions.assertEquals(440440L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(330330L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(220220L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(110110L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(11011L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testIncrementalWithNullStats() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.INCREMENTAL,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_WITH_NULL_FIELDS,
          STREAM_SYNC_STATS_2,
        ),
      )

    Assertions.assertEquals(440L, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(30330L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(220L, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(10110L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(11L, aggregatedStats.recordsRejected)
  }

  @Test
  fun testFullRefreshWithNullStats() {
    val aggregatedStats =
      getAggregatedStats(
        SyncMode.FULL_REFRESH,
        listOf(
          STREAM_SYNC_STATS_1,
          STREAM_SYNC_STATS_2,
          STREAM_SYNC_STATS_WITH_NULL_FIELDS,
        ),
      )

    Assertions.assertEquals(0, aggregatedStats.recordsEmitted)
    Assertions.assertEquals(30000L, aggregatedStats.bytesEmitted)
    Assertions.assertEquals(0, aggregatedStats.recordsCommitted)
    Assertions.assertEquals(10000L, aggregatedStats.bytesCommitted)
    Assertions.assertEquals(0, aggregatedStats.recordsRejected)
  }

  @Test
  fun testStreamWasBackfilled() {
    val wasBackfilled: Optional<Boolean> =
      wasBackfilled(listOf(STREAM_SYNC_STATS_1, STREAM_SYNC_STATS_2, STREAM_SYNC_STATS_3))
    Assertions.assertTrue(wasBackfilled.isPresent())
    Assertions.assertTrue(wasBackfilled.get())
  }

  @Test
  fun testStreamWasNotBackfilled() {
    val wasBackfilled: Optional<Boolean> = wasBackfilled(listOf(STREAM_SYNC_STATS_2, STREAM_SYNC_STATS_3))
    Assertions.assertTrue(wasBackfilled.isPresent())
    Assertions.assertFalse(wasBackfilled.get())
  }

  @Test
  fun testBackfillNotSpecified() {
    val wasBackfilled: Optional<Boolean> = wasBackfilled(listOf(STREAM_SYNC_STATS_3))
    Assertions.assertTrue(wasBackfilled.isEmpty())
  }

  companion object {
    private val STREAM_SYNC_STATS_1: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(40L)
            .withBytesEmitted(30L)
            .withRecordsCommitted(20L)
            .withBytesCommitted(10L)
            .withRecordsRejected(1L),
        ).withWasBackfilled(true)

    private val STREAM_SYNC_STATS_2: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(400L)
            .withBytesEmitted(300L)
            .withRecordsCommitted(200L)
            .withBytesCommitted(100L)
            .withRecordsRejected(10L),
        ).withWasBackfilled(false)

    private val STREAM_SYNC_STATS_3: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(4000L)
            .withBytesEmitted(3000L)
            .withRecordsCommitted(2000L)
            .withBytesCommitted(1000L)
            .withRecordsRejected(100L),
        )

    private val STREAM_SYNC_STATS_4_RESUMED: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(40000L)
            .withBytesEmitted(30000L)
            .withRecordsCommitted(20000L)
            .withBytesCommitted(10000L)
            .withRecordsRejected(1000L),
        ).withWasResumed(true)

    private val STREAM_SYNC_STATS_5_RESUMED: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(400000L)
            .withBytesEmitted(300000L)
            .withRecordsCommitted(200000L)
            .withBytesCommitted(100000L)
            .withRecordsRejected(10000L),
        ).withWasResumed(true)

    private val STREAM_SYNC_STATS_WITH_NULL_FIELDS: StreamSyncStats =
      StreamSyncStats()
        .withStreamName("")
        .withStats(
          SyncStats()
            .withRecordsEmitted(null)
            .withBytesEmitted(30000L)
            .withRecordsCommitted(null)
            .withBytesCommitted(10000L)
            .withRecordsRejected(null),
        )
  }
}
