package io.airbyte.workload.launcher.pipeline

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.mocks.LauncherInputMessage

/**
 * Input/Output object for LaunchPipeline.
 * @param msg - input msg
 * @param skip - whether to skip the stage
 */
sealed class StageIO {
  abstract val msg: LauncherInputMessage
  var skip: Boolean = false
}

data class LaunchStageIO(
  override val msg: LauncherInputMessage,
  var replicationInput: ReplicationInput? = null,
) : StageIO()
