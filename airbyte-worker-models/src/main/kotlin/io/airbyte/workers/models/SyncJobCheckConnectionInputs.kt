package io.airbyte.workers.models

import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig

/**
 * GeneratedJobInput.
 */
data class SyncJobCheckConnectionInputs(
  val sourceLauncherConfig: IntegrationLauncherConfig? = null,
  val destinationLauncherConfig: IntegrationLauncherConfig? = null,
  val sourceCheckConnectionInput: StandardCheckConnectionInput? = null,
  val destinationCheckConnectionInput: StandardCheckConnectionInput? = null,
)
