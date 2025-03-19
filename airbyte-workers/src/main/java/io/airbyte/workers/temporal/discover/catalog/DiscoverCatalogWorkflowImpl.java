/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.temporal.failure.ApplicationFailure;

/**
 * DiscoverCatalogWorkflowImpl.
 */
public class DiscoverCatalogWorkflowImpl implements DiscoverCatalogWorkflow {

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig launcherConfig,
                                final StandardDiscoverCatalogInput config) {
    throw ApplicationFailure.newNonRetryableFailure("Workflow has been deprecated. Retry to automatically transition to the current workflow.",
        "Deprecated");
  }

}
