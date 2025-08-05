/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.StandardSyncOutput
import io.temporal.api.enums.v1.RetryState
import io.temporal.failure.ActivityFailure

class ReplicateFailureSyncWorkflow : SyncWorkflowV2 {
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput =
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

  companion object {
    // Should match activity types from FailureHelper.java
    private const val ACTIVITY_TYPE_REPLICATE = "Replicate"

    val CAUSE: Throwable = Exception("replicate failed")
  }
}
