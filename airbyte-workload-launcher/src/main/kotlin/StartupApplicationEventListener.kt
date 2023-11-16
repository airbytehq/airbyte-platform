/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RESUME_CLAIMED_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.model.toLauncherInput
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton
import kotlin.concurrent.thread

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
    thread {
      try {
        retrieveAndProcessClaimed()
      } catch (e: Exception) {
        logger.error(e) { "rehydrateAndProcessClaimed failed" }
      }

      workerFactory.start()
    }
  }

  @VisibleForTesting
  @Trace(operationName = RESUME_CLAIMED_OPERATION_NAME)
  fun retrieveAndProcessClaimed() {
    addTagsToTrace()
    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.CLAIMED),
      )

    val workloadList: WorkloadListResponse =
      apiClient.workloadList(workloadListRequest)

    logger.info { "Re-hydrating ${workloadList.workloads.size} workload claim(s)..." }

    workloadList.workloads.forEach {
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_ON_RESTART,
        MetricAttribute(WORKLOAD_ID_TAG, it.id),
      )
      pipe.processClaimed(it.toLauncherInput())
    }
  }

  private fun addTagsToTrace() {
    val commonTags = hashMapOf<String, Any>()
    commonTags[DATA_PLANE_ID_TAG] = dataplaneId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }
}
