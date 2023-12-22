package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.CheckConnectionInput

fun CheckConnectionInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun CheckConnectionInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun CheckConnectionInput.usesCustomConnector(): Boolean {
  return this.launcherConfig.isCustomConnector
}

fun CheckConnectionInput.setConnectorLabels(labels: Map<String, String>): CheckConnectionInput {
  return this.apply {
    launcherConfig.additionalLabels = labels
  }
}
