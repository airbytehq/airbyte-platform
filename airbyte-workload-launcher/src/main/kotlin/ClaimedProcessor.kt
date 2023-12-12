/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RESUME_CLAIMED_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.model.toLauncherInput
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toFlux

private val logger = KotlinLogging.logger {}

@Singleton
class ClaimedProcessor(
  private val apiClient: WorkloadApi,
  private val pipe: LaunchPipeline,
  private val metricPublisher: CustomMetricPublisher,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  @Value("\${airbyte.workload-launcher.parallelism}") parallelism: Int,
) {
  private val scheduler = Schedulers.newParallel("process-claimed-scheduler", parallelism)

  @Trace(operationName = RESUME_CLAIMED_OPERATION_NAME)
  fun retrieveAndProcess() {
    addTagsToTrace()
    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.CLAIMED),
      )

    val workloadList: WorkloadListResponse =
      apiClient.workloadList(workloadListRequest)

    logger.info { "Re-hydrating ${workloadList.workloads.size} workload claim(s)..." }

    val msgs = workloadList.workloads.map { it.toLauncherInput() }

    processMessages(msgs)
  }

  @VisibleForTesting
  fun processMessages(msgs: List<LauncherInput>) {
    msgs.map { runOnClaimedScheduler(it) }
      .toFlux()
      .flatMap { w -> w }
      .collectList()
      .block()
  }

  private fun runOnClaimedScheduler(msg: LauncherInput): Mono<LaunchStageIO> {
    metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_CLAIM_RESUMED, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
    return pipe.buildPipeline(msg)
      .subscribeOn(scheduler)
  }

  private fun addTagsToTrace() {
    val commonTags = hashMapOf<String, Any>()
    commonTags[DATA_PLANE_ID_TAG] = dataplaneId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }
}
