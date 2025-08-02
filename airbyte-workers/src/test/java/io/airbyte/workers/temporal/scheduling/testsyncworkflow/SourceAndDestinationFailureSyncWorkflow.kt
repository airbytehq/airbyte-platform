/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import org.assertj.core.util.Sets
import java.util.UUID

class SourceAndDestinationFailureSyncWorkflow : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput =
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

  override fun checkAsyncActivityStatus() {}

  companion object {
    @VisibleForTesting
    val FAILURE_REASONS: MutableSet<FailureReason?> =
      Sets.newLinkedHashSet<FailureReason?>(
        FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withTimestamp(System.currentTimeMillis()),
        FailureReason().withFailureOrigin(FailureReason.FailureOrigin.DESTINATION).withTimestamp(System.currentTimeMillis()),
      )
  }
}
