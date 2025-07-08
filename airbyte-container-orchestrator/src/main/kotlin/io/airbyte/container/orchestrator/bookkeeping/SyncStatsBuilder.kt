/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:JvmName("SyncStatsBuilder")

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats

/**
 * Extract StreamStats from SyncStatsTracker.
 *
 * TODO the notion of hasReplicationCompleted feels obsolete, we should simplify and always return the count
 */
fun SyncStatsTracker.getPerStreamStats(hasReplicationCompleted: Boolean): List<StreamSyncStats> =
  getStats()
    .map { (stream, stats) ->
      val syncStats =
        SyncStats()
          // TODO Handle Rejected Records when Available
          .withBytesCommitted(stats.bytesCommitted)
          .withBytesEmitted(stats.bytesEmitted)
          .withBytesFilteredOut(stats.bytesFilteredOut)
          .withEstimatedBytes(stats.bytesEstimated)
          .withEstimatedRecords(stats.recordsEstimated)
          .withRecordsCommitted(stats.recordsCommitted)
          .withRecordsEmitted(stats.recordsEmitted)
          .withRecordsFilteredOut(stats.recordsFilteredOut)
          .withRecordsRejected(stats.recordsRejected.takeIf { it > 0 })
          .apply {
            if (hasReplicationCompleted) {
              bytesCommitted = stats.bytesEmitted - stats.bytesFilteredOut
              recordsCommitted = stats.recordsEmitted - stats.recordsFilteredOut - stats.recordsRejected
            } else {
              bytesCommitted = stats.bytesCommitted
              recordsCommitted = stats.recordsCommitted
            }
          }

      StreamSyncStats()
        .withStreamName(stream.name)
        .withStreamNamespace(stream.namespace)
        .withStats(syncStats)
    }

private data class TotalStats(
  var recordsCommitted: Long = 0,
  var recordsEmitted: Long = 0,
  var recordsFilteredOut: Long = 0,
  var recordsRejected: Long = 0,
  var bytesCommitted: Long = 0,
  var bytesEmitted: Long = 0,
  var bytesFilteredOut: Long = 0,
) {
  fun add(other: SyncStats) {
    recordsCommitted += other.recordsCommitted
    recordsEmitted += other.recordsEmitted
    recordsFilteredOut += other.recordsFilteredOut
    recordsRejected += other.recordsRejected ?: 0
    bytesCommitted += other.bytesCommitted
    bytesEmitted += other.bytesEmitted
    bytesFilteredOut += other.bytesFilteredOut
  }
}

/**
 * Extract total stats from SyncStatsTracker.
 *
 * TODO the notion of hasReplicationCompleted feels obsolete, we should simplify and always return the count
 */
fun SyncStatsTracker.getTotalStats(
  streamStats: List<StreamSyncStats>,
  hasReplicationCompleted: Boolean,
): SyncStats {
  val totalStats = TotalStats()
  streamStats.forEach {
    totalStats.add(it.stats)
  }
  return SyncStats()
    .withRecordsEmitted(totalStats.recordsEmitted)
    .withRecordsFilteredOut(totalStats.recordsFilteredOut)
    .withBytesEmitted(totalStats.bytesEmitted)
    .withBytesFilteredOut(totalStats.bytesFilteredOut)
    .withSourceStateMessagesEmitted(getTotalSourceStateMessagesEmitted())
    .withDestinationStateMessagesEmitted(getTotalDestinationStateMessagesEmitted())
    .withMaxSecondsBeforeSourceStateMessageEmitted(getMaxSecondsToReceiveSourceStateMessage())
    .withMeanSecondsBeforeSourceStateMessageEmitted(getMeanSecondsToReceiveSourceStateMessage())
    .withMaxSecondsBetweenStateMessageEmittedandCommitted(getMaxSecondsBetweenStateMessageEmittedAndCommitted())
    .withMeanSecondsBetweenStateMessageEmittedandCommitted(getMeanSecondsBetweenStateMessageEmittedAndCommitted())
    .apply {
      if (totalStats.recordsRejected > 0) {
        recordsRejected = totalStats.recordsRejected
      }

      // TODO Handle Rejected Records when Available
      if (hasReplicationCompleted) {
        bytesCommitted = bytesEmitted.minus(bytesFilteredOut)
        recordsCommitted = recordsEmitted.minus(recordsFilteredOut).minus(totalStats.recordsRejected)
      } else {
        bytesCommitted = totalStats.bytesCommitted
        recordsCommitted = totalStats.recordsCommitted
      }
    }
}
