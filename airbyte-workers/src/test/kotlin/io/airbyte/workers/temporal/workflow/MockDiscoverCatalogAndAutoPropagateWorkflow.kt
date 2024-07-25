package io.airbyte.workers.temporal.workflow

import io.airbyte.commons.temporal.scheduling.DiscoverCatalogAndAutoPropagateWorkflow
import io.airbyte.config.CatalogDiff
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput

class MockDiscoverCatalogAndAutoPropagateWorkflow : DiscoverCatalogAndAutoPropagateWorkflow {
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): RefreshSchemaActivityOutput {
    assert(config.actorContext.organizationId != null)
    return REFRESH_SCHEMA_ACTIVITY_OUTPUT
  }

  companion object {
    @JvmField
    val REFRESH_SCHEMA_ACTIVITY_OUTPUT =
      RefreshSchemaActivityOutput(
        CatalogDiff().withAdditionalProperty("test", "test"),
      )
  }
}
