/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.config.StreamSyncStats;
import io.airbyte.protocol.models.SyncMode;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
        return getAggregatedStatsForFullRefresh(streamStats);
      case INCREMENTAL:
        return getAggregatedStatsForIncremental(streamStats);
      default:
        throw new IllegalArgumentException("Unknown sync mode: " + syncMode);
    }
  }

  private static StreamStatsRecord getAggregatedStatsForFullRefresh(List<StreamSyncStats> streamStats) {
    StreamSyncStats mostRecentStats = streamStats.getLast();
    return new StreamStatsRecord(
        mostRecentStats.getStreamName(),
        mostRecentStats.getStreamNamespace(),
        mostRecentStats.getStats().getRecordsEmitted(),
        mostRecentStats.getStats().getBytesEmitted(),
        mostRecentStats.getStats().getRecordsCommitted(),
        mostRecentStats.getStats().getBytesCommitted(),
        wasBackfilled(streamStats));
  }

  private static StreamStatsRecord getAggregatedStatsForIncremental(List<StreamSyncStats> streamStats) {
    return new StreamStatsRecord(
        streamStats.getFirst().getStreamName(),
        streamStats.getFirst().getStreamNamespace(),
        streamStats.stream().mapToLong(s -> s.getStats().getRecordsEmitted()).sum(),
        streamStats.stream().mapToLong(s -> s.getStats().getBytesEmitted()).sum(),
        streamStats.stream().mapToLong(s -> s.getStats().getRecordsCommitted()).sum(),
        streamStats.stream().mapToLong(s -> s.getStats().getBytesCommitted()).sum(),
        wasBackfilled(streamStats));
  }

  static Optional<Boolean> wasBackfilled(List<StreamSyncStats> streamStats) {
    streamStats.stream().filter(x -> x.getWasBackfilled() != null).collect(Collectors.toList());
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
