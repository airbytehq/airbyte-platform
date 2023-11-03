package io.airbyte.workload.launcher.model

import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.ReplicationInput

fun ReplicationInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun ReplicationInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun ReplicationInput.getOrchestratorResourceReqs(): ResourceRequirements? {
  return this.syncResourceRequirements?.orchestrator
}

fun ReplicationInput.usesCustomConnector(): Boolean {
  return this.sourceLauncherConfig.isCustomConnector || this.destinationLauncherConfig.isCustomConnector
}
