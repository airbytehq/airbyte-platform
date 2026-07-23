/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec

import io.airbyte.commons.temporal.scheduling.SpecWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.opentelemetry.instrumentation.annotations.WithSpan
import io.temporal.failure.ApplicationFailure

/**
 * SpecWorkflowImpl.
 */
open class SpecWorkflowImpl : SpecWorkflow {
  @WithSpan
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
  ): ConnectorJobOutput =
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
}
