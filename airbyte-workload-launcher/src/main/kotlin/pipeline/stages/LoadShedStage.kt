/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedWorkloadLauncher
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

@Singleton
@Named("loadShed")
open class LoadShedStage(
  private val featureFlagClient: FeatureFlagClient,
  private val workloadClient: WorkloadApiClient,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "LoadShedStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "loadShed")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val shouldShed = featureFlagClient.boolVariation(LoadShedWorkloadLauncher, input.ffContext!!)
    if (shouldShed) {
      try {
        workloadClient.updateStatusToFailed(input.msg.workloadId, LOAD_SHED_FAILURE_REASON)
      } catch (e: Exception) {
        logger.warn { "Failed to fail workload: ${input.msg.workloadId} as part of load shed." }
      }
    }

    return input.apply {
      skip = shouldShed
    }
  }

  override fun getStageName(): StageName = StageName.LOAD_SHED

  companion object {
    const val LOAD_SHED_FAILURE_REASON = "Workload was failed because the associated context is being load shed."
  }
}
