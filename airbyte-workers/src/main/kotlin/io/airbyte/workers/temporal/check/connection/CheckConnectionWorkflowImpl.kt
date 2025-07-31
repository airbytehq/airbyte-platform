/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.scheduling.CheckConnectionWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.failure.ApplicationFailure

/**
 * Check connection temporal workflow implementation.
 */
open class CheckConnectionWorkflowImpl : CheckConnectionWorkflow {
  @Trace(operationName = ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    connectionConfiguration: StandardCheckConnectionInput,
  ): ConnectorJobOutput =
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
}
