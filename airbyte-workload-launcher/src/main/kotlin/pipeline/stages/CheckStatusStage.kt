package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val LOGGER = KotlinLogging.logger {}

@Singleton
class CheckStatusStage(
  private val statusClient: StatusUpdater,
  private val kubeClient: KubePodClient,
) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    return if (kubeClient.podsExistForWorkload(input.msg.workloadId)) {
      LOGGER.info {
        "Found pods running for workload ${input.msg.workloadId}, setting status as running and skip flag as true"
      }
      statusClient.updateStatusToRunning(input.msg.workloadId)
      input.apply {
        skip = true
      }
    } else {
      LOGGER.info { "No pod found running for workload ${input.msg.workloadId}" }
      input
    }
  }

  override fun getStageName(): StageName {
    return StageName.CHECK_STATUS
  }
}
