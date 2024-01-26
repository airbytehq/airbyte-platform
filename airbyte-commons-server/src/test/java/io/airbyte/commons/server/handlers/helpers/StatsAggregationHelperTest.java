/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.wasBackfilled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.StreamStatsRecord;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.protocol.models.SyncMode;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsAggregationHelperTest {

  private static final StreamSyncStats STREAM_SYNC_STATS_1 = new StreamSyncStats()
      .withStats(new SyncStats()
          .withRecordsEmitted(10L)
          .withBytesEmitted(50L)
          .withRecordsCommitted(10L)
          .withBytesCommitted(50L))
      .withWasBackfilled(true);

  private static final StreamSyncStats STREAM_SYNC_STATS_2 = new StreamSyncStats()
      .withStats(new SyncStats()
          .withRecordsEmitted(100L)
          .withBytesEmitted(500L)
          .withRecordsCommitted(100L)
          .withBytesCommitted(500L))
      .withWasBackfilled(false);

  private static final StreamSyncStats STREAM_SYNC_STATS_3 = new StreamSyncStats()
      .withStats(new SyncStats()
          .withRecordsEmitted(1000L)
          .withBytesEmitted(5000L)
          .withRecordsCommitted(1000L)
          .withBytesCommitted(5000L));

  @Test
  void testAggregatedStatsFullRefresh() {
    StreamStatsRecord aggregatedStats = StatsAggregationHelper.getAggregatedStats(
        SyncMode.FULL_REFRESH,
        List.of(
            STREAM_SYNC_STATS_1,
            STREAM_SYNC_STATS_2));

    assertEquals(100L, aggregatedStats.recordsEmitted());
    assertEquals(500L, aggregatedStats.bytesEmitted());
    assertEquals(100L, aggregatedStats.recordsCommitted());
    assertEquals(500L, aggregatedStats.bytesCommitted());
  }

  @Test
  void testIncremental() {
    StreamStatsRecord aggregatedStats = StatsAggregationHelper.getAggregatedStats(
        SyncMode.INCREMENTAL,
        List.of(
            STREAM_SYNC_STATS_1,
            STREAM_SYNC_STATS_2));

    assertEquals(110L, aggregatedStats.recordsEmitted());
    assertEquals(550L, aggregatedStats.bytesEmitted());
    assertEquals(110L, aggregatedStats.recordsCommitted());
    assertEquals(550L, aggregatedStats.bytesCommitted());
  }

  @Test
  void testStreamWasBackfilled() {
    Optional<Boolean> wasBackfilled = wasBackfilled(List.of(STREAM_SYNC_STATS_1, STREAM_SYNC_STATS_2, STREAM_SYNC_STATS_3));
    assertTrue(wasBackfilled.isPresent());
    assertTrue(wasBackfilled.get());
  }

  @Test
  void testStreamWasNotBackfilled() {
    Optional<Boolean> wasBackfilled = wasBackfilled(List.of(STREAM_SYNC_STATS_2, STREAM_SYNC_STATS_3));
    assertTrue(wasBackfilled.isPresent());
    assertFalse(wasBackfilled.get());
  }

  @Test
  void testBackfillNotSpecified() {
    Optional<Boolean> wasBackfilled = wasBackfilled(List.of(STREAM_SYNC_STATS_3));
    assertTrue(wasBackfilled.isEmpty());
  }

}
