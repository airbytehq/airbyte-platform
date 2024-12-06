package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.DiscoverCatalogInput
import java.util.UUID

fun DiscoverCatalogInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun DiscoverCatalogInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun DiscoverCatalogInput.usesCustomConnector(): Boolean {
  return this.launcherConfig.isCustomConnector
}

fun DiscoverCatalogInput.getOrganizationId(): UUID {
  return this.discoverCatalogInput.actorContext.organizationId
}
