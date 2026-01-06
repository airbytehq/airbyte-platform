/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.commons.temporal.scheduling.DiscoverCatalogWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.opentelemetry.instrumentation.annotations.WithSpan
import io.temporal.failure.ApplicationFailure

/**
 * DiscoverCatalogWorkflowImpl.
 */
open class DiscoverCatalogWorkflowImpl : DiscoverCatalogWorkflow {
  @WithSpan
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): ConnectorJobOutput =
    throw ApplicationFailure.newNonRetryableFailure(
      "Workflow has been deprecated. Retry to automatically transition to the current workflow.",
      "Deprecated",
    )
}
