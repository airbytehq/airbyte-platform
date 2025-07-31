/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.workers.temporal.FailureConverter

/**
 * Static helpers for handling sync output.
 */
object SyncOutputProvider {
  @JvmField
  val EMPTY_FAILED_SYNC: StandardSyncSummary? =
    StandardSyncSummary()
      .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
      .withStartTime(System.currentTimeMillis())
      .withEndTime(System.currentTimeMillis())
      .withRecordsSynced(0L)
      .withBytesSynced(0L)
      .withTotalStats(
        SyncStats()
          .withRecordsEmitted(0L)
          .withBytesEmitted(0L)
          .withSourceStateMessagesEmitted(0L)
          .withDestinationStateMessagesEmitted(0L)
          .withRecordsCommitted(0L),
      )

  /**
   * Get refresh schema failure.
   *
   * @param e exception that caused the failure
   * @return sync output
   */
  fun getRefreshSchemaFailure(e: Exception): StandardSyncOutput {
    val failure = FailureConverter().getFailureReason("Refresh Schema", ActorType.SOURCE, e)
    if (failure.failureType == null) {
      failure.failureType = FailureReason.FailureType.REFRESH_SCHEMA
    }
    return StandardSyncOutput()
      .withFailures(listOf(failure))
      .withStandardSyncSummary(EMPTY_FAILED_SYNC)
  }
}
