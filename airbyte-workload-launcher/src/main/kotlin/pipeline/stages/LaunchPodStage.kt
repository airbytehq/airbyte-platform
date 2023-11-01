package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class LaunchPodStage(private val launcher: KubePodClient) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { "Stage: ${javaClass.simpleName}" }
    val replInput = input.replicationInput!!

    launcher.launchReplication(replInput, input.msg.workloadId)

    return input
  }

  override fun getStageName(): StageName {
    return StageName.LAUNCH
  }
}
