package io.airbyte.workload.launcher.pods

import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.MUTEX_KEY
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import jakarta.inject.Singleton

@Singleton
class PodLabeler {
  fun getSourceLabels(): Map<String, String> {
    return mapOf(
      SYNC_STEP_KEY to READ_STEP,
    )
  }

  fun getDestinationLabels(): Map<String, String> {
    return mapOf(
      SYNC_STEP_KEY to WRITE_STEP,
    )
  }

  fun getOrchestratorLabels(): Map<String, String> {
    return mapOf(
      SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
    )
  }

  fun getWorkloadLabels(workloadId: String): Map<String, String> {
    return mapOf(
      WORKLOAD_ID to workloadId,
    )
  }

  fun getMutexLabels(key: String?): Map<String, String> {
    if (key == null) {
      return mapOf()
    }

    return mapOf(
      MUTEX_KEY to key,
    )
  }

  fun getSharedLabels(
    workloadId: String,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
  ): Map<String, String> {
    return passThroughLabels + getMutexLabels(mutexKey) + getWorkloadLabels(workloadId)
  }
}
