package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.MUTEX_KEY
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import jakarta.inject.Singleton

@Singleton
class PodLabeler {
  fun getSourceLabels(
    input: ReplicationInput,
    workloadId: String,
  ): Map<String, String> {
    return getSharedLabels(input, workloadId) +
      mapOf(
        SYNC_STEP_KEY to READ_STEP,
      )
  }

  fun getDestinationLabels(
    input: ReplicationInput,
    workloadId: String,
  ): Map<String, String> {
    return getSharedLabels(input, workloadId) +
      mapOf(
        SYNC_STEP_KEY to WRITE_STEP,
      )
  }

  fun getOrchestratorLabels(
    input: ReplicationInput,
    workloadId: String,
  ): Map<String, String> {
    return getSharedLabels(input, workloadId) +
      mapOf(
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )
  }

  fun getWorkloadLabels(workloadId: String): Map<String, String> {
    return mapOf(
      WORKLOAD_ID to workloadId,
    )
  }

  fun getMutexLabels(input: ReplicationInput): Map<String, String> {
    return mapOf(
      MUTEX_KEY to getMutexKey(input),
    )
  }

  fun getSharedLabels(
    input: ReplicationInput,
    workloadId: String,
  ): Map<String, String> {
    return getMutexLabels(input) + getWorkloadLabels(workloadId)
  }

  // TODO: this should be passed from workload API
  private fun getMutexKey(input: ReplicationInput): String {
    return input.connectionId.toString()
  }
}
