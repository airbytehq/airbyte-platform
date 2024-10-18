package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

/**
 * Input/Output object for LaunchPipeline.
 */
sealed class StageIO {
  /** msg - input message */
  abstract val msg: LauncherInput

  /** logCtx - string key value pairs to add to logging context */
  abstract val logCtx: Map<String, String>

  /** skip - whether to skip the stage */
  var skip: Boolean = false
}

/**
 * Input/Output object for LaunchPipeline.
 * @param msg - input msg
 * @param logCtx - string key value pairs to add to logging context
 * @param payload - workload payload
 */
data class LaunchStageIO(
  override val msg: LauncherInput,
  override val logCtx: Map<String, String> = mapOf(),
  var payload: WorkloadPayload? = null,
) : StageIO()

sealed class WorkloadPayload

data class SyncPayload(
  var input: ReplicationInput,
) : WorkloadPayload()

data class CheckPayload(
  var input: CheckConnectionInput,
) : WorkloadPayload()

data class DiscoverCatalogPayload(
  var input: DiscoverCatalogInput,
) : WorkloadPayload()

data class SpecPayload(
  var input: SpecInput,
) : WorkloadPayload()
