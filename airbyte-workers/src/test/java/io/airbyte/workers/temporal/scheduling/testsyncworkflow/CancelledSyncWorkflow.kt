/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import io.airbyte.config.SyncStats
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import java.util.UUID

class CancelledSyncWorkflow : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput {
    return StandardSyncOutput()
      .withStandardSyncSummary(
        StandardSyncSummary()
          .withStatus(ReplicationStatus.CANCELLED)
          .withTotalStats(SyncStats()),
      )
  }

  override fun checkAsyncActivityStatus() {}
}
