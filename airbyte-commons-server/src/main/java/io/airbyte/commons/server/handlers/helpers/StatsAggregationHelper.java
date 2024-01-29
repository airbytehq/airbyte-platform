/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.protocol.models.SyncMode;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Helper class to aggregate stream stats. The class is meant to be used to aggregate stats for a
 * single stream across multiple attempts
 */
public class StatsAggregationHelper {

  /**
   * WARNING! billing uses the stats that this method returns. Be careful when changing this method.
   *
   * Aggregates stream stats based on the given sync mode. The given stream stats should be a list of
   * stats for a single stream across multiple attempts. The list must be sorted by attempt id in
   * ascending order. The given sync mode should be the sync mode of the stream of which the stream
   * stats belong to.
   *
   * How stats are aggregated depends on the given sync mode
   * <ul>
   * <li>Full refresh: the aggregated stats equals to the stats of the last element of the given
   * stream stats list</li>
   * <li>Incremental: the aggregated stats are the sum of the stats of all stream stats</li>
   * </ul>
   *
   * @param syncMode stream sync mode
   * @param streamStats stream attempt stats. Should have at least one element
   * @return aggregated stats for the given stream
   */
  public static StreamStatsRecord getAggregatedStats(SyncMode syncMode, List<StreamSyncStats> streamStats) {
    switch (syncMode) {
      case FULL_REFRESH:
        StreamSyncStats lastStreamSyncStats = streamStats.getLast();
        return getAggregatedStats(Collections.singletonList(lastStreamSyncStats));
      case INCREMENTAL:
        return getAggregatedStats(streamStats);
      default:
        throw new IllegalArgumentException("Unknown sync mode: " + syncMode);
    }
  }

  private static StreamStatsRecord getAggregatedStats(List<StreamSyncStats> streamStats) {
    long recordsEmitted = 0;
    long bytesEmitted = 0;
    long recordsCommitted = 0;
    long bytesCommitted = 0;

    for (StreamSyncStats streamStat : streamStats) {
      SyncStats syncStats = streamStat.getStats();
      recordsEmitted += syncStats.getRecordsEmitted() == null ? 0 : syncStats.getRecordsEmitted();
      bytesEmitted += syncStats.getBytesEmitted() == null ? 0 : syncStats.getBytesEmitted();
      recordsCommitted += syncStats.getRecordsCommitted() == null ? 0 : syncStats.getRecordsCommitted();
      bytesCommitted += syncStats.getBytesCommitted() == null ? 0 : syncStats.getBytesCommitted();
    }

    return new StreamStatsRecord(
        streamStats.getFirst().getStreamName(),
        streamStats.getFirst().getStreamNamespace(),
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        wasBackfilled(streamStats));
  }

  static Optional<Boolean> wasBackfilled(List<StreamSyncStats> streamStats) {
    // if a stream is a backfill, at least one attempt will be marked as backfill
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasBackfilled() != null && syncStats.getWasBackfilled())) {
      return Optional.of(true);
    }

    // if no attempts were marked as backfill then either the stream is not a backfill or
    // the backfill flag hasn't been set yet (flag is set when the attempt completes)
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasBackfilled() != null && !syncStats.getWasBackfilled())) {
      return Optional.of(false);
    }

    return Optional.empty();
  }

  public record StreamStatsRecord(String streamName,
                                  String streamNamespace,
                                  Long recordsEmitted,
                                  Long bytesEmitted,
                                  Long recordsCommitted,
                                  Long bytesCommitted,
                                  Optional<Boolean> wasBackfilled) {}

}
