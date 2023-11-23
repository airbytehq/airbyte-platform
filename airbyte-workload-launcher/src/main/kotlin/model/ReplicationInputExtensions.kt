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

fun ReplicationInput.setSourceLabels(labels: Map<String, String>): ReplicationInput {
  return this.apply {
    sourceLauncherConfig =
      sourceLauncherConfig.apply {
        additionalLabels = labels
      }
  }
}

fun ReplicationInput.setDestinationLabels(labels: Map<String, String>): ReplicationInput {
  return this.apply {
    destinationLauncherConfig =
      destinationLauncherConfig.apply {
        additionalLabels = labels
      }
  }
}
