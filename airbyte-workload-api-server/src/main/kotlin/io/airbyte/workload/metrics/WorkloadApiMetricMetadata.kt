/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
}
