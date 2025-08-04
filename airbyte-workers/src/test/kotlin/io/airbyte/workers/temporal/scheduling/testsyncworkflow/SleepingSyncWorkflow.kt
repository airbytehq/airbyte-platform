/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.workflow.Workflow
import java.time.Duration
import java.util.UUID

class SleepingSyncWorkflow : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput {
    Workflow.sleep(RUN_TIME)

    return StandardSyncOutput()
  }

  override fun checkAsyncActivityStatus() {}

  companion object {
    @JvmField
    val RUN_TIME: Duration? = Duration.ofMinutes(10L)
  }
}
