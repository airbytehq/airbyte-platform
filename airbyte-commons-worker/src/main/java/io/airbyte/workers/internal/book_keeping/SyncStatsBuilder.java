/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper function to prepare stats for persistence.
 */
@Slf4j
public class SyncStatsBuilder {

  /**
   * Extract StreamStats from SyncStatsTracker.
   */
  public static List<StreamSyncStats> getPerStreamStats(final SyncStatsTracker syncStatsTracker, final boolean hasReplicationCompleted) {
    // assume every stream with stats is in streamToEmittedRecords map
    return syncStatsTracker.getStreamToEmittedRecords().keySet().stream().map(stream -> {
      final SyncStats syncStats = new SyncStats()
          .withRecordsEmitted(syncStatsTracker.getStreamToEmittedRecords().get(stream))
          .withBytesEmitted(syncStatsTracker.getStreamToEmittedBytes().get(stream))
          .withSourceStateMessagesEmitted(null)
          .withDestinationStateMessagesEmitted(null);

      if (hasReplicationCompleted) {
        syncStats.setBytesCommitted(syncStatsTracker.getStreamToEmittedBytes().get(stream));
        syncStats.setRecordsCommitted(syncStatsTracker.getStreamToEmittedRecords().get(stream));
      } else {
        syncStats.setBytesCommitted(readValueIfPresent(syncStatsTracker.getStreamToCommittedBytes(), stream));
        syncStats.setRecordsCommitted(readValueIfPresent(syncStatsTracker.getStreamToCommittedRecords(), stream));
      }
      return new StreamSyncStats()
          .withStreamName(stream.getName())
          .withStreamNamespace(stream.getNamespace())
          .withStats(syncStats);
    }).collect(Collectors.toList());
  }

  private static Long readValueIfPresent(final Optional<Map<AirbyteStreamNameNamespacePair, Long>> optMap,
                                         final AirbyteStreamNameNamespacePair stream) {
    return optMap.isPresent() ? optMap.get().get(stream) : null;
  }

  /**
   * Extract total stats from SyncStatsTracker.
   */
  public static SyncStats getTotalStats(final SyncStatsTracker syncStatsTracker, final boolean hasReplicationCompleted) {
    final SyncStats totalSyncStats = new SyncStats()
        .withRecordsEmitted(syncStatsTracker.getTotalRecordsEmitted())
        .withBytesEmitted(syncStatsTracker.getTotalBytesEmitted())
        .withSourceStateMessagesEmitted(syncStatsTracker.getTotalSourceStateMessagesEmitted())
        .withDestinationStateMessagesEmitted(syncStatsTracker.getTotalDestinationStateMessagesEmitted())
        .withMaxSecondsBeforeSourceStateMessageEmitted(syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage())
        .withMeanSecondsBeforeSourceStateMessageEmitted(syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage())
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted().orElse(null))
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted().orElse(null));

    if (hasReplicationCompleted) {
      totalSyncStats.setBytesCommitted(totalSyncStats.getBytesEmitted());
      totalSyncStats.setRecordsCommitted(totalSyncStats.getRecordsEmitted());
    } else {
      totalSyncStats.setBytesCommitted(syncStatsTracker.getTotalBytesCommitted().orElse(null));
      totalSyncStats.setRecordsCommitted(syncStatsTracker.getTotalRecordsCommitted().orElse(null));
    }
    return totalSyncStats;
  }

}
