/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.api.enums.v1.RetryState
import io.temporal.failure.ActivityFailure
import io.temporal.workflow.Workflow
import java.util.UUID

/**
 * Test sync that simulate an activity failure of the child workflow.
 */
class SyncWorkflowWithActivityFailureException : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput {
    Workflow.sleep(SleepingSyncWorkflow.Companion.RUN_TIME)
    throw ActivityFailure(
      "sync workflow activity failed",
      1L,
      1L,
      "Replication",
      "id",
      RetryState.RETRY_STATE_RETRY_POLICY_NOT_SET,
      "identity",
      Exception("Error"),
    )
  }

  override fun checkAsyncActivityStatus() {}
}
