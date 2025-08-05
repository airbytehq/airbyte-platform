/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.StandardSyncOutput
import io.temporal.workflow.Workflow
import java.time.Duration

class SleepingSyncWorkflow : SyncWorkflowV2 {
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput {
    Workflow.sleep(RUN_TIME)
    return StandardSyncOutput()
  }

  companion object {
    @JvmField
    val RUN_TIME: Duration? = Duration.ofMinutes(10L)
  }
}
