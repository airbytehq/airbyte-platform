package io.airbyte.workers.temporal.workflows

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogAndAutoPropagateWorkflow
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.temporal.failure.ApplicationFailure

open class DiscoverCatalogAndAutoPropagateWorkflowImpl : DiscoverCatalogAndAutoPropagateWorkflow {
  @Trace(operationName = ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): RefreshSchemaActivityOutput {
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
  }
}
