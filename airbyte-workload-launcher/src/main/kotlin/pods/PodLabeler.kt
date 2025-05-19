/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workers.pod.Metadata
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
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_KEY
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_VALUE
import jakarta.inject.Singleton
import java.util.UUID

internal const val AUTO_ID = "auto_id"
internal const val MUTEX_KEY = "mutex_key"
internal const val WORKLOAD_ID = "workload_id"
internal const val ORCHESTRATOR_IMAGE_NAME = "orchestrator_image_name"
internal const val ORCHESTRATOR_IMAGE_VERSION = "orchestrator_image_version"
internal const val SOURCE_IMAGE_NAME = "source_image_name"
internal const val SOURCE_IMAGE_VERSION = "source_image_version"
internal const val DESTINATION_IMAGE_NAME = "destination_image_name"
internal const val DESTINATION_IMAGE_VERSION = "destination_image_version"

@Singleton
class PodLabeler(
  private val podNetworkSecurityLabeler: PodNetworkSecurityLabeler,
) {
  fun getSourceLabels(): Map<String, String> =
    mapOf(
      SYNC_STEP_KEY to READ_STEP,
    )

  fun getDestinationLabels(): Map<String, String> =
    mapOf(
      SYNC_STEP_KEY to WRITE_STEP,
    )

  fun getReplicationOrchestratorLabels(orchestratorImageName: String): Map<String, String> =
    getOrchestratorImageLabels(orchestratorImageName) +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )

  fun getReplicationLabels(
    orchestratorImageName: String,
    sourceImageName: String,
    destImageName: String,
  ): Map<String, String> =
    getReplicationImageLabels(orchestratorImageName, sourceImageName, destImageName) +
      mapOf(
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to REPLICATION_STEP,
      )

  fun getCheckLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to CHECK_JOB,
    )

  fun getDiscoverLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to DISCOVER_JOB,
    )

  fun getSpecLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to SPEC_JOB,
    )

  fun getAutoIdLabels(autoId: UUID): Map<String, String> =
    mapOf(
      AUTO_ID to autoId.toString(),
    )

  fun getWorkloadLabels(workloadId: String?): Map<String, String> =
    workloadId?.let {
      mapOf(
        WORKLOAD_ID to it,
      )
    } ?: emptyMap()

  fun getMutexLabels(key: String?): Map<String, String> =
    key?.let {
      mapOf(
        MUTEX_KEY to it,
      )
    } ?: emptyMap()

  internal fun getPodSweeperLabels(): Map<String, String> =
    mapOf(
      SWEEPER_LABEL_KEY to SWEEPER_LABEL_VALUE,
    )

  fun getSharedLabels(
    workloadId: String?,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
    autoId: UUID,
    workspaceId: UUID?,
    networkSecurityTokens: List<String>,
  ): Map<String, String> =
    passThroughLabels +
      getMutexLabels(mutexKey) +
      getWorkloadLabels(workloadId) +
      getAutoIdLabels(autoId) +
      getPodSweeperLabels() +
      podNetworkSecurityLabeler.getLabels(workspaceId, networkSecurityTokens)

  private fun getReplicationImageLabels(
    orchestratorImageName: String,
    sourceImageName: String,
    destImageName: String,
  ): Map<String, String> {
    val (orchName, orchVersion) = getImageMetadataPair(orchestratorImageName)
    val (srcName, srcVersion) = getImageMetadataPair(sourceImageName)
    val (dstName, dstVersion) = getImageMetadataPair(destImageName)

    return mapOf(
      ORCHESTRATOR_IMAGE_NAME to orchName,
      ORCHESTRATOR_IMAGE_VERSION to orchVersion,
      SOURCE_IMAGE_NAME to srcName,
      SOURCE_IMAGE_VERSION to srcVersion,
      DESTINATION_IMAGE_NAME to dstName,
      DESTINATION_IMAGE_VERSION to dstVersion,
    )
  }

  private fun getOrchestratorImageLabels(imageName: String): Map<String, String> =
    getImageMetadataPair(imageName).let {
      mapOf(
        Metadata.IMAGE_NAME to it.first,
        Metadata.IMAGE_VERSION to it.second,
      )
    }

  private fun getImageMetadataPair(imageName: String): Pair<String, String> =
    shortImageName(imageName) to
      imageName.substringAfterLast(":", "latest")

  object LabelKeys {
    const val SWEEPER_LABEL_KEY = "airbyte"
    const val SWEEPER_LABEL_VALUE = "job-pod"
  }
}
