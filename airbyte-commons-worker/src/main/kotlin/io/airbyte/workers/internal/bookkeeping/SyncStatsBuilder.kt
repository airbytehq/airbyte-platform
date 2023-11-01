@file:JvmName("SyncStatsBuilder")

package io.airbyte.workers.internal.bookkeeping

import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats

/**
 * Extract StreamStats from SyncStatsTracker.
 */
fun SyncStatsTracker.getPerStreamStats(hasReplicationCompleted: Boolean): List<StreamSyncStats> {
  // for readability purposes, avoids having to use `this@getPerStreamStats` in the map below
  val tracker = this

  // assume every stream with stats is in streamToEmittedRecords map
  return getStreamToEmittedRecords().map { (stream, records) ->
    val syncStats: SyncStats =
      SyncStats()
        .withBytesEmitted(tracker.getStreamToEmittedBytes()[stream])
        .withRecordsEmitted(records)
        .withSourceStateMessagesEmitted(null)
        .withDestinationStateMessagesEmitted(null)
        .apply {
          if (hasReplicationCompleted) {
            bytesCommitted = tracker.getStreamToEmittedBytes()[stream]
            recordsCommitted = tracker.getStreamToEmittedRecords()[stream]
          } else {
            bytesCommitted = tracker.getStreamToCommittedBytes()[stream]
            recordsCommitted = tracker.getStreamToCommittedRecords()[stream]
          }
        }

    StreamSyncStats()
      .withStreamName(stream.name)
      .withStreamNamespace(stream.namespace)
      .withStats(syncStats)
  }
    .toList()
}

/**
 * Extract total stats from SyncStatsTracker.
 */
fun SyncStatsTracker.getTotalStats(hasReplicationCompleted: Boolean): SyncStats {
  return SyncStats()
    .withRecordsEmitted(getTotalRecordsEmitted())
    .withBytesEmitted(getTotalBytesEmitted())
    .withSourceStateMessagesEmitted(getTotalSourceStateMessagesEmitted())
    .withDestinationStateMessagesEmitted(getTotalDestinationStateMessagesEmitted())
    .withMaxSecondsBeforeSourceStateMessageEmitted(getMaxSecondsToReceiveSourceStateMessage())
    .withMeanSecondsBeforeSourceStateMessageEmitted(getMeanSecondsToReceiveSourceStateMessage())
    .withMaxSecondsBetweenStateMessageEmittedandCommitted(getMaxSecondsBetweenStateMessageEmittedAndCommitted())
    .withMeanSecondsBetweenStateMessageEmittedandCommitted(getMeanSecondsBetweenStateMessageEmittedAndCommitted())
    .apply {
      if (hasReplicationCompleted) {
        bytesCommitted = bytesEmitted
        recordsCommitted = recordsEmitted
      } else {
        bytesCommitted = getTotalBytesCommitted()
        recordsCommitted = getTotalRecordsCommitted()
      }
    }
}
