/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.DataplaneGetIdRequestBody
import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.metrics.CustomMetricPublisher
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata.Companion.QUEUE_NAME_TAG
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata.Companion.WORKLOAD_PUBLISHER_OPERATION_NAME
import io.airbyte.workload.metrics.WorkloadApiMetricMetadata.Companion.WORKLOAD_TYPE_TAG
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Placeholder class to interact with the launcher queue for testing.
 * Should be merged with the controller when ready.
 */
@Singleton
open class WorkloadService(
  private val messageProducer: TemporalMessageProducer<LauncherInputMessage>,
  private val metricPublisher: CustomMetricPublisher,
  private val airbyteApiClient: AirbyteApiClient,
) {
  companion object {
    const val CONNECTION_ID_LABEL_KEY = "connection_id"
    const val ACTOR_ID_LABEL_KEY = "actor_id"
    const val ACTOR_TYPE_LABEL_KEY = "actor_type"
    const val WORKSPACE_ID_LABEL_KEY = "workspace_id"
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
  ) {
    // TODO feature flag geography
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadId) as Map<String, Any>?)
    val queue = getQueueName(labels, priority)
    // TODO: We could pass through created_at, but I'm use using system time for now.
    // This may get just replaced by tracing at some point if we manage to set it up properly.
    val startTimeMs = System.currentTimeMillis()
    messageProducer.publish(
      queue,
      LauncherInputMessage(workloadId, workloadInput, labels, logPath, mutexKey, workloadType, startTimeMs, autoId),
      "wl-create_$workloadId",
    )
    metricPublisher.count(
      WorkloadApiMetricMetadata.WORKLOAD_MESSAGE_PUBLISHED.metricName,
      MetricAttribute(WORKLOAD_ID_TAG, workloadId),
      MetricAttribute(QUEUE_NAME_TAG, queue),
      MetricAttribute(WORKLOAD_TYPE_TAG, workloadType.toString()),
    )
  }

  private fun getQueueName(
    labels: Map<String, String>,
    priority: WorkloadPriority,
  ): String {
    val connectionId = labels[CONNECTION_ID_LABEL_KEY]
    val actorId = labels[ACTOR_ID_LABEL_KEY]
    val actorType = labels[ACTOR_TYPE_LABEL_KEY]
    val workspaceId = labels[WORKSPACE_ID_LABEL_KEY]
    val dataplaneGetIdRequestBody =
      DataplaneGetIdRequestBody(
        priority.toApiRequest(),
        connectionId?.let { UUID.fromString(it) },
        actorType?.let { ActorType.decode(it) },
        actorId?.let { UUID.fromString(it) },
        workspaceId?.let { UUID.fromString(it) },
      )
    return airbyteApiClient.dataplaneApi.getDataplaneId(dataplaneGetIdRequestBody).id
  }
}

fun WorkloadPriority.toApiRequest(): io.airbyte.api.client.model.generated.WorkloadPriority {
  return when (this) {
    WorkloadPriority.HIGH -> io.airbyte.api.client.model.generated.WorkloadPriority.HIGH
    WorkloadPriority.DEFAULT -> io.airbyte.api.client.model.generated.WorkloadPriority.DEFAULT
  }
}
