/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.api.enums.v1.TimeoutType
import io.temporal.failure.TimeoutFailure
import io.temporal.workflow.Workflow
import java.util.UUID

/**
 * Test sync workflow to simulate a hearbeat timeout. It will:
 *  *
 *
 * sleep for 10 minutes
 *
 *
 * throw a temporal timeout exception
 *
 *
 */
class SyncWorkflowFailingWithHearbeatTimeoutException : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput {
    Workflow.sleep(SleepingSyncWorkflow.Companion.RUN_TIME)
    throw TimeoutFailure("heartbeat timeout", null, TimeoutType.TIMEOUT_TYPE_HEARTBEAT)
  }

  override fun checkAsyncActivityStatus() {}
}
