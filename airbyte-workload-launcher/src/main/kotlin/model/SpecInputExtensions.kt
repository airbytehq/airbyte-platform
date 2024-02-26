package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.SpecInput

fun SpecInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun SpecInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun SpecInput.usesCustomConnector(): Boolean {
  return this.launcherConfig.isCustomConnector
}
