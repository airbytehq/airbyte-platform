/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.ClaimResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class ClaimStage(
  private val workloadApiClient: WorkloadApi,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  private val metricPublisher: CustomMetricPublisher,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val resp: ClaimResponse =
      workloadApiClient.workloadClaim(
        WorkloadClaimRequest(
          input.msg.workloadId,
          dataplaneId,
        ),
      )

    if (!resp.claimed) {
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
