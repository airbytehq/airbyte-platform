/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

/**
 * Launches the pods for the workload, serializing and passing through input
 * values via the injected pod client.
 */
@Singleton
@Named("launch")
open class LaunchPodStage(
  private val launcher: KubePodClient,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "LaunchPodStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "launch")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    when (val payload = input.payload!!) {
      is SyncPayload ->
        if (payload.input.isReset) {
          launcher.launchReset(payload, input.msg)
        } else {
          launcher.launchReplication(payload, input.msg)
        }
      is CheckPayload -> launcher.launchCheck(payload.input, input.msg)
      is DiscoverCatalogPayload -> launcher.launchDiscover(payload.input, input.msg)
      is SpecPayload -> launcher.launchSpec(payload.input, input.msg)
    }

    return input
  }

  override fun getStageName(): StageName = StageName.LAUNCH
}
