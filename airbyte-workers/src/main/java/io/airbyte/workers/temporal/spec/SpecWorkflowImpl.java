/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.scheduling.SpecWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.temporal.failure.ApplicationFailure;

/**
 * SpecWorkflowImpl.
 */
public class SpecWorkflowImpl implements SpecWorkflow {

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig, final IntegrationLauncherConfig launcherConfig) {
    throw ApplicationFailure.newNonRetryableFailure("Workflow has been deprecated. Retry to automatically transition to the current workflow.",
        "Deprecated");
  }

}
