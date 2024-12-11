package io.airbyte.workers.models

import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig

data class SidecarInput(
  val checkConnectionInput: StandardCheckConnectionInput?,
  val discoverCatalogInput: StandardDiscoverCatalogInput?,
  val workloadId: String,
  val integrationLauncherConfig: IntegrationLauncherConfig,
  val operationType: OperationType,
  val logPath: String,
) {
  enum class OperationType {
    CHECK,
    DISCOVER,
    SPEC,
  }
}
