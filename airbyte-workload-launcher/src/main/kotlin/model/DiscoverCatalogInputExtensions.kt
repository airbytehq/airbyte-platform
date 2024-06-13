package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.DiscoverCatalogInput

fun DiscoverCatalogInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun DiscoverCatalogInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun DiscoverCatalogInput.usesCustomConnector(): Boolean {
  return this.launcherConfig.isCustomConnector
}
