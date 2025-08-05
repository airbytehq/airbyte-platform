/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.failure.ApplicationFailure
import java.util.UUID

/**
 * Sync temporal workflow impl.
 */
open class SyncWorkflowImpl : SyncWorkflow {
  private var shouldBlock: Boolean? = null

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun checkAsyncActivityStatus() {
    this.shouldBlock = false
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput =
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
}
