package io.airbyte.workers.pod

import io.airbyte.workers.pod.PodLabeler.LabelKeys.AUTO_ID
import io.airbyte.workers.pod.PodLabeler.LabelKeys.MUTEX_KEY
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SWEEPER_LABEL_KEY
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SWEEPER_LABEL_VALUE
import io.airbyte.workers.pod.PodLabeler.LabelKeys.WORKLOAD_ID
import io.airbyte.workers.process.Metadata
import io.airbyte.workers.process.Metadata.CHECK_JOB
import io.airbyte.workers.process.Metadata.DISCOVER_JOB
import io.airbyte.workers.process.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SPEC_JOB
import io.airbyte.workers.process.Metadata.SYNC_JOB
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workers.process.ProcessFactory
import jakarta.inject.Singleton
import java.util.UUID

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

  fun getReplicationOrchestratorLabels(orchestratorImageName: String): Map<String, String> {
    return getImageLabels(orchestratorImageName) +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )
  }

  fun getCheckLabels(): Map<String, String> {
    return mapOf(
      JOB_TYPE_KEY to CHECK_JOB,
    )
  }

  fun getDiscoverLabels(): Map<String, String> {
    return mapOf(
      JOB_TYPE_KEY to DISCOVER_JOB,
    )
  }

  fun getSpecLabels(): Map<String, String> {
    return mapOf(
      JOB_TYPE_KEY to SPEC_JOB,
    )
  }

  private fun getImageLabels(orchestratorImageName: String): Map<String, String> {
    val shortImageName = ProcessFactory.getShortImageName(orchestratorImageName)
    val imageVersion = ProcessFactory.getImageVersion(orchestratorImageName)

    return mapOf(
      Metadata.IMAGE_NAME to shortImageName,
      Metadata.IMAGE_VERSION to imageVersion,
    )
  }

  fun getAutoIdLabels(autoId: UUID): Map<String, String> {
    return mapOf(
      AUTO_ID to autoId.toString(),
    )
  }

  fun getWorkloadLabels(workloadId: String?): Map<String, String> {
    if (workloadId == null) {
      return mapOf()
    }

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

  fun getPodSweeperLabels(): Map<String, String> {
    return mapOf(
      SWEEPER_LABEL_KEY to SWEEPER_LABEL_VALUE,
    )
  }

  fun getSharedLabels(
    workloadId: String?,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
    autoId: UUID,
  ): Map<String, String> {
    return passThroughLabels +
      getMutexLabels(mutexKey) +
      getWorkloadLabels(workloadId) +
      getAutoIdLabels(autoId) +
      getPodSweeperLabels()
  }

  object LabelKeys {
    const val AUTO_ID = "auto_id"
    const val MUTEX_KEY = "mutex_key"
    const val WORKLOAD_ID = "workload_id"
    const val SWEEPER_LABEL_KEY = "airbyte"
    const val SWEEPER_LABEL_VALUE = "job-pod"
  }
}
