/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:JvmName("SyncStatsBuilder")

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats

/**
 * Extract StreamStats from SyncStatsTracker.
 */
fun SyncStatsTracker.getPerStreamStats(hasReplicationCompleted: Boolean): List<StreamSyncStats> {
  // for readability purposes, avoids having to use `this@getPerStreamStats` in the map below
  val tracker = this

  // assume every stream with stats is in streamToEmittedRecords map
  return getStreamToEmittedRecords()
    .map { (stream, records) ->
      val syncStats: SyncStats =
        SyncStats()
          .withBytesEmitted(tracker.getStreamToEmittedBytes()[stream])
          .withRecordsEmitted(records)
          .withRecordsFilteredOut(tracker.getStreamToFilteredOutRecords()[stream])
          .withBytesFilteredOut(tracker.getStreamToFilteredOutBytes()[stream])
          .withSourceStateMessagesEmitted(null)
          .withDestinationStateMessagesEmitted(null)
          .apply {
            if (hasReplicationCompleted) {
              bytesCommitted = tracker.getStreamToEmittedBytes()[stream]?.minus(bytesFilteredOut)
              recordsCommitted = tracker.getStreamToEmittedRecords()[stream]?.minus(recordsFilteredOut)
            } else {
              bytesCommitted = tracker.getStreamToCommittedBytes()[stream]
              recordsCommitted = tracker.getStreamToCommittedRecords()[stream]
            }
          }

      StreamSyncStats()
        .withStreamName(stream.name)
        .withStreamNamespace(stream.namespace)
        .withStats(syncStats)
    }.toList()
}

/**
 * Extract total stats from SyncStatsTracker.
 */
fun SyncStatsTracker.getTotalStats(hasReplicationCompleted: Boolean): SyncStats =
  SyncStats()
    .withRecordsEmitted(getTotalRecordsEmitted())
    .withRecordsFilteredOut(getTotalRecordsFilteredOut())
    .withBytesEmitted(getTotalBytesEmitted())
    .withBytesFilteredOut(getTotalBytesFilteredOut())
    .withSourceStateMessagesEmitted(getTotalSourceStateMessagesEmitted())
    .withDestinationStateMessagesEmitted(getTotalDestinationStateMessagesEmitted())
    .withMaxSecondsBeforeSourceStateMessageEmitted(getMaxSecondsToReceiveSourceStateMessage())
    .withMeanSecondsBeforeSourceStateMessageEmitted(getMeanSecondsToReceiveSourceStateMessage())
    .withMaxSecondsBetweenStateMessageEmittedandCommitted(getMaxSecondsBetweenStateMessageEmittedAndCommitted())
    .withMeanSecondsBetweenStateMessageEmittedandCommitted(getMeanSecondsBetweenStateMessageEmittedAndCommitted())
    .apply {
      if (hasReplicationCompleted) {
        bytesCommitted = bytesEmitted.minus(bytesFilteredOut)
        recordsCommitted = recordsEmitted.minus(recordsFilteredOut)
      } else {
        bytesCommitted = getTotalBytesCommitted()
        recordsCommitted = getTotalRecordsCommitted()
      }
    }
