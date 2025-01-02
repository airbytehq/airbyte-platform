/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.metrics

import io.airbyte.metrics.lib.MetricEmittingApp
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.metrics.lib.MetricsRegistry

enum class WorkloadApiMetricMetadata(
  private val metricName: String,
  private val description: String,
) : MetricsRegistry {
  WORKLOAD_MESSAGE_PUBLISHED(
    "workload_message_published",
    "Count of workloads published to the queue",
  ),
  ;

  override fun getApplication(): MetricEmittingApp {
    return MetricEmittingApps.WORKLOAD_API
  }

  override fun getMetricName(): String {
    return metricName
  }

  override fun getMetricDescription(): String {
    return description
  }

  companion object {
    const val DATA_PLANE_ID_TAG = "data_plane_id"
    const val GEOGRAPHY_TAG = "geography"
    const val MUTEX_KEY_TAG = "mutex_key"
    const val QUEUE_NAME_TAG = "queue_name"
    const val WORKLOAD_ID_TAG = "workload_id"
    const val WORKLOAD_TYPE_TAG = "workload_type"
    const val WORKLOAD_CANCEL_REASON_TAG = "cancel_reason"
    const val WORKLOAD_CANCEL_SOURCE_TAG = "cancel_source"
    const val WORKLOAD_PUBLISHER_OPERATION_NAME = "workload_publisher"
  }
}
