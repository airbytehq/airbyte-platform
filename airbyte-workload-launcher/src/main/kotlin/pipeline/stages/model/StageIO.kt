/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.featureflag.Context
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import kotlin.time.TimeSource

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
 * @param ffContext - feature flag context derived from the input payload
 */
data class LaunchStageIO(
  override val msg: LauncherInput,
  override val logCtx: Map<String, String> = mapOf(),
  var payload: WorkloadPayload? = null,
  var ffContext: Context? = null,
  var receivedAt: TimeSource.Monotonic.ValueTimeMark? = null,
) : StageIO() {
  val workloadId = msg.workloadId
}

sealed class WorkloadPayload

data class SyncPayload(
  var input: ReplicationInput,
  var architectureEnvironmentVariables: ArchitectureEnvironmentVariables? = null,
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
