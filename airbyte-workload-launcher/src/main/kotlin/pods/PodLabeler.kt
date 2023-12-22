package io.airbyte.workload.launcher.pods

import io.airbyte.workers.process.Metadata
import io.airbyte.workers.process.Metadata.CHECK_JOB
import io.airbyte.workers.process.Metadata.CHECK_STEP_KEY
import io.airbyte.workers.process.Metadata.CONNECTOR_STEP
import io.airbyte.workers.process.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_CHECK_STEP
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_JOB
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workers.process.ProcessFactory
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.MUTEX_KEY
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.WORKLOAD_ID
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class PodLabeler(
  @Named("containerOrchestratorImage") private val orchestratorImageName: String,
) {
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

  fun getReplicationOrchestratorLabels(): Map<String, String> {
    return getImageLabels() +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )
  }

  fun getCheckConnectorLabels(): Map<String, String> {
    return mapOf(
      JOB_TYPE_KEY to CHECK_JOB,
      CHECK_STEP_KEY to CONNECTOR_STEP,
    )
  }

  fun getCheckOrchestratorLabels(): Map<String, String> {
    return getImageLabels() +
      mapOf(
        JOB_TYPE_KEY to CHECK_JOB,
        CHECK_STEP_KEY to ORCHESTRATOR_CHECK_STEP,
      )
  }

  private fun getImageLabels(): Map<String, String> {
    val shortImageName = ProcessFactory.getShortImageName(orchestratorImageName)
    val imageVersion = ProcessFactory.getImageVersion(orchestratorImageName)

    return mapOf(
      Metadata.IMAGE_NAME to shortImageName,
      Metadata.IMAGE_VERSION to imageVersion,
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

  object LabelKeys {
    const val WORKLOAD_ID = "workload_id"
    const val MUTEX_KEY = "mutex_key"
  }
}
