/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_TYPE_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
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
  metricPublisher: CustomMetricPublisher,
  @Value("\${airbyte.data-plane-id}") dataplaneId: String,
) : LaunchStage(metricPublisher, dataplaneId) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "ClaimStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MeterFilterFactory.STAGE_NAME_TAG, value = "claim")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val claimed = apiClient.claim(input.msg.workloadId)

    if (!claimed) {
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_NOT_CLAIMED,
        MetricAttribute(WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
      )
      logger.info { "Workload not claimed. Setting SKIP flag to true." }
      return input.apply {
        skip = true
      }
    }

    metricPublisher.count(
      WorkloadLauncherMetricMetadata.WORKLOAD_CLAIMED,
      MetricAttribute(WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
    )
    return input
  }

  override fun getStageName(): StageName {
    return StageName.CLAIM
  }
}
