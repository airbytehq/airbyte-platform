/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Calls the workload broker to try and claim ownership of the workload. If we
 * successfully claim the workload we continue, otherwise we no-op to the end of
 * the pipeline.
 */
@Singleton
@Named("claim")
class ClaimStage(
  private val apiClient: WorkloadApiClient,
  private val metricPublisher: CustomMetricPublisher,
) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val claimed = apiClient.claim(input.msg.workloadId)

    if (!claimed) {
      metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_NOT_CLAIMED, MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId))
      logger.info { "Workload not claimed. Setting SKIP flag to true." }
      return input.apply {
        skip = true
      }
    }

    metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_CLAIMED, MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId))
    return input
  }

  override fun getStageName(): StageName {
    return StageName.CLAIM
  }
}
