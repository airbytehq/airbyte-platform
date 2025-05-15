/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.ArchitectureDecider
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Pipeline stage to compute and attach architecture ENV vars.
 */
@Singleton
@Named("architecture")
class ArchitectureDeciderStage(
  metricClient: MetricClient,
  private val architectureDecider: ArchitectureDecider,
) : LaunchStage(metricClient) {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    input.payload?.takeIf { it is SyncPayload }?.let {
      val payload = it as SyncPayload
      payload.architectureEnvironmentVariables =
        architectureDecider.computeEnvironmentVariables(payload.input)
    }
    return input
  }

  override fun getStageName(): StageName = StageName.ARCHITECTURE
}
