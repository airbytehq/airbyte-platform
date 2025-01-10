/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.input

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

fun ReplicationInput.getSourceResourceReqs(): ResourceRequirements? {
  return this.syncResourceRequirements?.source
}

fun ReplicationInput.getDestinationResourceReqs(): ResourceRequirements? {
  return this.syncResourceRequirements?.destination
}

fun ReplicationInput.usesCustomConnector(): Boolean {
  return this.sourceLauncherConfig.isCustomConnector || this.destinationLauncherConfig.isCustomConnector
}
