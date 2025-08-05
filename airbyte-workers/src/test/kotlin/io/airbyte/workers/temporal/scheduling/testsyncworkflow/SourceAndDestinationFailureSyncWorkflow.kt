/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import org.assertj.core.util.Sets

class SourceAndDestinationFailureSyncWorkflow : SyncWorkflowV2 {
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput =
    StandardSyncOutput()
      .withFailures(FAILURE_REASONS.stream().toList())
      .withStandardSyncSummary(
        StandardSyncSummary()
          .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
          .withTotalStats(
            SyncStats()
              .withRecordsCommitted(10L) // should lead to partialSuccess = true
              .withRecordsEmitted(20L),
          ),
      )

  companion object {
    @VisibleForTesting
    val FAILURE_REASONS: MutableSet<FailureReason?> =
      Sets.newLinkedHashSet<FailureReason?>(
        FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withTimestamp(System.currentTimeMillis()),
        FailureReason().withFailureOrigin(FailureReason.FailureOrigin.DESTINATION).withTimestamp(System.currentTimeMillis()),
      )
  }
}
