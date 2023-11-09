/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.Workload
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.REHYDRATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class StartupApplicationEventListener(
  private val apiClient: WorkloadApi,
  private val pipe: LaunchPipeline,
  private val workerFactory: WorkerFactory,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  private val metricPublisher: CustomMetricPublisher,
) :
  ApplicationEventListener<ServiceReadyEvent> {
  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    // TODO this might slowdown start quite a bit, should be reworked
    rehydrateAndProcessClaimed()

    workerFactory.start()
  }

  @VisibleForTesting
  @Trace(operationName = REHYDRATION_OPERATION_NAME)
  fun rehydrateAndProcessClaimed() {
    addTagsToTrace()
    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.cLAIMED),
      )

    val workloadList: WorkloadListResponse =
      apiClient.workloadList(workloadListRequest)

    workloadList.workloads.forEach {
      metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_ON_RESTART, MetricAttribute(WORKLOAD_ID_TAG, it.id))
      pipe.accept(convertToInputMessage(it))
    }
  }

  private fun addTagsToTrace() {
    val commonTags = hashMapOf<String, Any>()
    commonTags.put(DATA_PLANE_ID_TAG, dataplaneId)
    ApmTraceUtils.addTagsToTrace(commonTags)
  }

  @VisibleForTesting
  fun convertToInputMessage(workload: Workload): LauncherInput {
    // TODO(Subodh): Add proper input once the format is decided
    return LauncherInput(workload.id, "workload-input", "log-path.txt")
  }
}
