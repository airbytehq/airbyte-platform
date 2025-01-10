package io.airbyte.workers.models

import io.airbyte.config.StandardSyncInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig

data class JobInput(
  val jobRunConfig: JobRunConfig? = null,
  val sourceLauncherConfig: IntegrationLauncherConfig? = null,
  val destinationLauncherConfig: IntegrationLauncherConfig? = null,
  val syncInput: StandardSyncInput? = null,
)
