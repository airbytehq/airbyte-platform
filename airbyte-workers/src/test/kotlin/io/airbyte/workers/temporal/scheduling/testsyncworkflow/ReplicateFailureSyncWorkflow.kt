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
import java.util.UUID

class ReplicateFailureSyncWorkflow : SyncWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput =
    throw ActivityFailure(
      "replicate failed",
      1L,
      1L,
      ACTIVITY_TYPE_REPLICATE,
      "someId",
      RetryState.RETRY_STATE_UNSPECIFIED,
      "someIdentity",
      CAUSE,
    )

  override fun checkAsyncActivityStatus() {}

  companion object {
    // Should match activity types from FailureHelper.java
    private const val ACTIVITY_TYPE_REPLICATE = "Replicate"

    val CAUSE: Throwable = Exception("replicate failed")
  }
}
