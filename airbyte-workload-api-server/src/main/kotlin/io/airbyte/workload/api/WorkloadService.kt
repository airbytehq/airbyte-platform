/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.WorkloadType
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.metrics.CustomMetricPublisher
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.QUEUE_NAME_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_PUBLISHER_OPERATION_NAME
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata
import jakarta.inject.Singleton

/**
 * Placeholder class to interact with the launcher queue for testing.
 * Should be merged with the controller when ready.
 */
@Singleton
open class WorkloadService(
  private val messageProducer: TemporalMessageProducer<LauncherInputMessage>,
  private val metricPublisher: CustomMetricPublisher,
  private val featureFlagClient: FeatureFlagClient,
) {
  @Trace(operationName = WORKLOAD_PUBLISHER_OPERATION_NAME)
  open fun create(
    workloadId: String,
    workloadInput: String,
    labels: Map<String, String>,
    logPath: String,
    geography: String,
    mutexKey: String?,
    workloadType: WorkloadType,
  ) {
    // TODO feature flag geography
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadId) as Map<String, Any>?)
    val queue = getQueueName(geography)
    // TODO: We could pass through created_at, but I'm use using system time for now.
    // This may get just replaced by tracing at some point if we manage to set it up properly.
    val startTimeMs = System.currentTimeMillis()
    messageProducer.publish(
      queue,
      LauncherInputMessage(workloadId, workloadInput, labels, logPath, mutexKey, workloadType, startTimeMs),
      "wl-create_$workloadId",
    )
    metricPublisher.count(
      WorkloadApiMetricMetadata.WORKLOAD_MESSAGE_PUBLISHED.metricName,
      MetricAttribute(WORKLOAD_ID_TAG, workloadId),
      MetricAttribute(QUEUE_NAME_TAG, queue),
    )
  }

  private fun getQueueName(geography: String): String {
    val context = Geography(geography)
    return featureFlagClient.stringVariation(WorkloadApiRouting, context)
  }
}
