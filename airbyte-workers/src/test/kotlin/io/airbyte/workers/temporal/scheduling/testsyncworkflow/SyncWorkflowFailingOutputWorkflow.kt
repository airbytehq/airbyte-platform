/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary

class SyncWorkflowFailingOutputWorkflow : SyncWorkflowV2 {
  /**
   * Return an output that report a failure without throwing an exception. This failure is not a
   * partial success.
   */
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput =
    StandardSyncOutput()
      .withStandardSyncSummary(
        StandardSyncSummary()
          .withStatus(StandardSyncSummary.ReplicationStatus.FAILED),
      )
}
