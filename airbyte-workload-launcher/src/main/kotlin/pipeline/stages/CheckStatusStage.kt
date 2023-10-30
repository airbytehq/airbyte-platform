package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.api.client2.AirbyteApiClient2
import io.airbyte.api.client2.model.generated.WorkloadStatus
import io.airbyte.api.client2.model.generated.WorkloadStatusUpdateRequest
import io.airbyte.workload.launcher.client.KubeClient
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val LOGGER = KotlinLogging.logger {}

@Singleton
class CheckStatusStage(
  private val airbyteClient: AirbyteApiClient2,
  private val kubeClient: KubeClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    return if (kubeClient.podsExistForWorkload(input.msg.workloadId, namespace)) {
      LOGGER.info {
        "Found pods running for workload ${input.msg.workloadId}, setting status as running and skip flag as true"
      }
      val successfullyUpdatedStatusToRunning = updateStatusToRunning(input.msg.workloadId)
      input.apply {
        skip = successfullyUpdatedStatusToRunning
      }
    } else {
      LOGGER.info { "No pod found running for workload ${input.msg.workloadId}" }
      input
    }
  }

  private fun updateStatusToRunning(workloadId: String): Boolean {
    try {
      val workloadStatusUpdateRequest =
        WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.running)
      airbyteClient.workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest)
    } catch (e: Exception) {
      LOGGER.warn(e) { "Could not set the status for workload $workloadId to running even after re-tries" }
    }
    return true
  }

  override fun getStageName(): StageName {
    return StageName.CHECK_STATUS
  }
}
