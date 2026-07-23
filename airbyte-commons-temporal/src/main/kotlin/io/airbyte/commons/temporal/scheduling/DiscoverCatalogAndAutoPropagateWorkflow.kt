/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@WorkflowInterface
interface DiscoverCatalogAndAutoPropagateWorkflow {
  @WorkflowMethod
  fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): RefreshSchemaActivityOutput
}
