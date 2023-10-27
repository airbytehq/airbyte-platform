package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class CheckStatusStage : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { "Stage: ${javaClass.simpleName}" }

    return input
  }

  override fun getStageName(): StageName {
    return StageName.CHECK_STATUS
  }
}
