/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.client.LogContextFactory
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import jakarta.inject.Singleton
import kotlin.time.TimeSource

/**
 * Transforms LauncherInput into StageIO and performs other ingress related side-effects.
 */
@Singleton
class PipelineIngressAdapter(
  private val identityService: DataplaneIdentityService,
  private val metricClient: MetricClient,
  private val ctxFactory: LogContextFactory,
) {
  fun apply(input: LauncherInput): LaunchStageIO {
    ingestMetrics(input)
    return inputToStageIO(input)
  }

  private fun inputToStageIO(input: LauncherInput): LaunchStageIO {
    val loggingCtx = ctxFactory.create(input)
    return LaunchStageIO(input, loggingCtx, receivedAt = TimeSource.Monotonic.markNow())
  }

  private fun ingestMetrics(input: LauncherInput): LauncherInput {
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_RECEIVED,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.workloadType.toString()),
        ),
    )

    val commonTags = hashMapOf<String, Any>()
    commonTags[MetricTags.DATA_PLANE_ID_TAG] = identityService.getDataplaneId()
    commonTags[MetricTags.DATA_PLANE_NAME_TAG] = identityService.getDataplaneName()
    commonTags[MetricTags.WORKLOAD_ID_TAG] = input.workloadId
    ApmTraceUtils.addTagsToTrace(commonTags.toMap())

    return input
  }
}
