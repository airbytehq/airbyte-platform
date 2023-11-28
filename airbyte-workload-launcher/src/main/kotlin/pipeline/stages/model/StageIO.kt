package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

/**
 * Input/Output object for LaunchPipeline.
 * @param msg - input msg
 * @param skip - whether to skip the stage
 */
sealed class StageIO {
  abstract val msg: LauncherInput
  abstract val logPath: String
  var skip: Boolean = false
}

data class LaunchStageIO(
  override val msg: LauncherInput,
  var replicationInput: ReplicationInput? = null,
) : StageIO() {
  override val logPath: String = msg.logPath
}
