package io.airbyte.workers.pod

import io.airbyte.workers.pod.Metadata.CHECK_JOB
import io.airbyte.workers.pod.Metadata.DISCOVER_JOB
import io.airbyte.workers.pod.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.pod.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.pod.Metadata.READ_STEP
import io.airbyte.workers.pod.Metadata.REPLICATION_STEP
import io.airbyte.workers.pod.Metadata.SPEC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.pod.Metadata.WRITE_STEP
import io.airbyte.workers.pod.PodLabeler.LabelKeys.AUTO_ID
import io.airbyte.workers.pod.PodLabeler.LabelKeys.DESTINATION_IMAGE_NAME
import io.airbyte.workers.pod.PodLabeler.LabelKeys.DESTINATION_IMAGE_VERSION
import io.airbyte.workers.pod.PodLabeler.LabelKeys.MUTEX_KEY
import io.airbyte.workers.pod.PodLabeler.LabelKeys.ORCHESTRATOR_IMAGE_NAME
import io.airbyte.workers.pod.PodLabeler.LabelKeys.ORCHESTRATOR_IMAGE_VERSION
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SOURCE_IMAGE_NAME
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SOURCE_IMAGE_VERSION
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SWEEPER_LABEL_KEY
import io.airbyte.workers.pod.PodLabeler.LabelKeys.SWEEPER_LABEL_VALUE
import io.airbyte.workers.pod.PodLabeler.LabelKeys.WORKLOAD_ID
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class PodLabeler(private val podNetworkSecurityLabeler: PodNetworkSecurityLabeler) {
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
    return getOrchestratorImageLabels(orchestratorImageName) +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )
  }

  fun getReplicationLabels(
    orchestratorImageName: String,
    sourceImageName: String,
    destImageName: String,
  ): Map<String, String> {
    return getReplicationImageLabels(orchestratorImageName, sourceImageName, destImageName) +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to REPLICATION_STEP,
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
    workspaceId: UUID?,
    networkSecurityTokens: List<String>,
  ): Map<String, String> {
    return passThroughLabels +
      getMutexLabels(mutexKey) +
      getWorkloadLabels(workloadId) +
      getAutoIdLabels(autoId) +
      getPodSweeperLabels() +
      podNetworkSecurityLabeler.getLabels(workspaceId, networkSecurityTokens)
  }

  fun getReplicationImageLabels(
    orchestratorImageName: String,
    sourceImageName: String,
    destImageName: String,
  ): Map<String, String> {
    val orchPair = getImageMetadataPair(orchestratorImageName)
    val sourcePair = getImageMetadataPair(sourceImageName)
    val destPair = getImageMetadataPair(destImageName)
    return mapOf(
      ORCHESTRATOR_IMAGE_NAME to orchPair.first,
      ORCHESTRATOR_IMAGE_VERSION to orchPair.second,
      SOURCE_IMAGE_NAME to sourcePair.first,
      SOURCE_IMAGE_VERSION to sourcePair.second,
      DESTINATION_IMAGE_NAME to destPair.first,
      DESTINATION_IMAGE_VERSION to destPair.second,
    )
  }

  private fun getOrchestratorImageLabels(imageName: String): Map<String, String> {
    val pair = getImageMetadataPair(imageName)

    return mapOf(
      Metadata.IMAGE_NAME to pair.first,
      Metadata.IMAGE_VERSION to pair.second,
    )
  }

  private fun getImageMetadataPair(imageName: String): Pair<String, String> {
    val shortImageName = PodUtils.getShortImageName(imageName)
    val imageVersion = PodUtils.getImageVersion(imageName)

    return shortImageName to imageVersion
  }

  object LabelKeys {
    const val AUTO_ID = "auto_id"
    const val MUTEX_KEY = "mutex_key"
    const val WORKLOAD_ID = "workload_id"
    const val ORCHESTRATOR_IMAGE_NAME = "orchestrator_image_name"
    const val ORCHESTRATOR_IMAGE_VERSION = "orchestrator_image_version"
    const val SOURCE_IMAGE_NAME = "source_image_name"
    const val SOURCE_IMAGE_VERSION = "source_image_version"
    const val DESTINATION_IMAGE_NAME = "destination_image_name"
    const val DESTINATION_IMAGE_VERSION = "destination_image_version"
    const val SWEEPER_LABEL_KEY = "airbyte"
    const val SWEEPER_LABEL_VALUE = "job-pod"
  }
}
