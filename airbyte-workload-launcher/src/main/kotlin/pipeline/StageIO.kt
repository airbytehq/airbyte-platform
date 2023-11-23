package io.airbyte.workload.launcher.pipeline

import io.airbyte.persistence.job.models.ReplicationInput

/**
 * Input/Output object for LaunchPipeline.
 * @param msg - input msg
 * @param skip - whether to skip the stage
 */
sealed class StageIO {
  abstract val msg: LauncherInput
  var skip: Boolean = false
}

data class LaunchStageIO(
  override val msg: LauncherInput,
  var replicationInput: ReplicationInput? = null,
) : StageIO()
