package io.airbyte.workload.launcher.model

import io.airbyte.config.ActorType
import io.airbyte.workers.models.CheckConnectionInput

fun CheckConnectionInput.getJobId(): String {
  return this.jobRunConfig.jobId
}

fun CheckConnectionInput.getAttemptId(): Long {
  return this.jobRunConfig.attemptId
}

fun CheckConnectionInput.getActorType(): ActorType {
  return this.checkConnectionInput.actorType
}

fun CheckConnectionInput.setConnectorLabels(labels: Map<String, String>): CheckConnectionInput {
  return this.apply {
    launcherConfig =
      launcherConfig.apply {
        additionalLabels = labels
      }
  }
}
