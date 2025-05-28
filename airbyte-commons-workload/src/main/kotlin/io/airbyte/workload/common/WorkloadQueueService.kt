/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.common

import datadog.trace.api.Trace
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.repository.WorkloadQueueRepository
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Placeholder class to interact with the launcher queue for testing.
 * Should be merged with the controller when ready.
 */
@Singleton
open class WorkloadQueueService(
  private val metricClient: MetricClient,
  private val workloadQueueRepository: WorkloadQueueRepository,
) {
  companion object {
    const val WORKLOAD_PUBLISHER_OPERATION_NAME: String = "workload_publisher"
  }

  @Trace(operationName = WORKLOAD_PUBLISHER_OPERATION_NAME)
  open fun create(
    workloadId: String,
    workloadInput: String,
    labels: Map<String, String>,
    logPath: String,
    mutexKey: String?,
    workloadType: WorkloadType,
    autoId: UUID,
    priority: WorkloadPriority,
    dataplaneGroup: String?,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadId) as Map<String, Any>?)

    // TODO dataplaneGroup should not be nullable
    if (dataplaneGroup != null) {
      workloadQueueRepository.enqueueWorkload(dataplaneGroup = dataplaneGroup, priority = priority.toInt(), workloadId = workloadId)

      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_MESSAGE_PUBLISHED,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, dataplaneGroup),
            MetricAttribute(MetricTags.PRIORITY_TAG, priority.name),
            MetricAttribute(MetricTags.WORKLOAD_ID_TAG, workloadId),
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workloadType.toString()),
          ),
      )
    }
  }
}
