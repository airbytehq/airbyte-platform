/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
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

/**
 * Calls the workload broker to try and claim ownership of the workload. If we
 * successfully claim the workload we continue, otherwise we no-op to the end of
 * the pipeline.
 */
@Singleton
@Named("claim")
open class ClaimStage(
  private val apiClient: WorkloadApiClient,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "ClaimStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "claim")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val claimed = apiClient.claim(input.msg.workloadId)

    if (!claimed) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_NOT_CLAIMED,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
          ),
      )
      logger.info { "Workload not claimed. Setting SKIP flag to true." }
      return input.apply {
        skip = true
      }
    }

    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_CLAIMED,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
        ),
    )
    return input
  }

  override fun getStageName(): StageName = StageName.CLAIM
}
