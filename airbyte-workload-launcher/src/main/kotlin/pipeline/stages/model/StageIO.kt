package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

/**
 * Input/Output object for LaunchPipeline.
 * @param msg - input msg
 * @param logCtx - string key value pairs to add to logging context
 * @param skip - whether to skip the stage
 */
sealed class StageIO {
  abstract val msg: LauncherInput
  abstract val logCtx: Map<String, String>
  var skip: Boolean = false
}

data class LaunchStageIO(
  override val msg: LauncherInput,
  override val logCtx: Map<String, String> = mapOf(),
  var replicationInput: ReplicationInput? = null,
) : StageIO()
