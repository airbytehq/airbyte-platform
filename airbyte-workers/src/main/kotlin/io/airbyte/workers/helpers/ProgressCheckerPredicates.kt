/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.AttemptStats
import jakarta.annotation.Nonnull
import jakarta.inject.Singleton

/**
 * Simple predicates for judging progress based on domain data.
 */
@Singleton
class ProgressCheckerPredicates {
  /**
   * Checks progress for AttemptStats.
   *
   * @param stats AttemptStats — must not be null.
   * @return true if we consider progress to have been made, false otherwise.
   */
  fun test(
    @Nonnull stats: AttemptStats,
  ): Boolean {
    // If recordsCommitted is null, treat this as a 0
    val recordsCommitted: Long = stats.recordsCommitted ?: 0L
    val recordsRejected: Long = stats.recordsRejected ?: 0L

    // In the context of progress, we count both committed and rejected records since rejected records
    // are
    // records that were successfully sent to the destination and rejected by the destination due to the
    // records
    // not confirming to the destination requirements. They are being tracked as part of checkpointing
    // and we should
    // be continuing past those records to avoid getting stuck on those.
    return recordsCommitted + recordsRejected >= RECORDS_COMMITTED_THRESHOLD
  }

  companion object {
    const val RECORDS_COMMITTED_THRESHOLD: Long = 1
  }
}
