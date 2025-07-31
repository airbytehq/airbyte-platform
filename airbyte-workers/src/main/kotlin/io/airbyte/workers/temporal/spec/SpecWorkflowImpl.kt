/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.scheduling.SpecWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.failure.ApplicationFailure

/**
 * SpecWorkflowImpl.
 */
open class SpecWorkflowImpl : SpecWorkflow {
  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
  ): ConnectorJobOutput =
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
}
