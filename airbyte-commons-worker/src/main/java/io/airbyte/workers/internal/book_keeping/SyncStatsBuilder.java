/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import java.util.List;
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
        syncStats.setRecordsCommitted(syncStatsTracker.getStreamToEmittedRecords().get(stream));
      } else if (syncStatsTracker.getStreamToCommittedRecords().isPresent()) {
        syncStats.setRecordsCommitted(syncStatsTracker.getStreamToCommittedRecords().get().get(stream));
      } else {
        syncStats.setRecordsCommitted(null);
      }
      return new StreamSyncStats()
          .withStreamName(stream.getName())
          .withStreamNamespace(stream.getNamespace())
          .withStats(syncStats);
    }).collect(Collectors.toList());
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
      totalSyncStats.setRecordsCommitted(totalSyncStats.getRecordsEmitted());
    } else if (syncStatsTracker.getTotalRecordsCommitted().isPresent()) {
      totalSyncStats.setRecordsCommitted(syncStatsTracker.getTotalRecordsCommitted().get());
    } else {
      log.warn("Could not reliably determine committed record counts, committed record stats will be set to null");
      totalSyncStats.setRecordsCommitted(null);
    }
    return totalSyncStats;
  }

}
