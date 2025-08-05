/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import io.airbyte.config.SyncStats

class CancelledSyncWorkflow : SyncWorkflowV2 {
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput =
    StandardSyncOutput()
      .withStandardSyncSummary(
        StandardSyncSummary()
          .withStatus(ReplicationStatus.CANCELLED)
          .withTotalStats(SyncStats()),
      )
}
