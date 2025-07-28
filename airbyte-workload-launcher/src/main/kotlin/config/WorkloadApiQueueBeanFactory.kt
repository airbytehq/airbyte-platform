/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePoller
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class WorkloadApiQueueBeanFactory {
  @Singleton
  @Named("highPriorityQueuePoller")
  fun highPriorityQueuePoller(
    workloadApiClient: WorkloadApiClient,
    metricClient: MetricClient,
    featureFlagClient: FeatureFlagClient,
    @Value("\${airbyte.workload-launcher.consumer.high-priority-queue.poll-size-items}") pollSizeItems: Int,
    @Value("\${airbyte.workload-launcher.consumer.high-priority-queue.poll-interval-seconds}") pollIntervalSeconds: Long,
  ): WorkloadApiQueuePoller =
    WorkloadApiQueuePoller(
      workloadApiClient,
      metricClient,
      featureFlagClient,
      pollSizeItems,
      pollIntervalSeconds,
      WorkloadPriority.HIGH,
    )

  @Singleton
  @Named("defaultPriorityQueuePoller")
  fun defaultPriorityQueuePoller(
    workloadApiClient: WorkloadApiClient,
    metricClient: MetricClient,
    featureFlagClient: FeatureFlagClient,
    @Value("\${airbyte.workload-launcher.consumer.default-queue.poll-size-items}") pollSizeItems: Int,
    @Value("\${airbyte.workload-launcher.consumer.default-queue.poll-interval-seconds}") pollIntervalSeconds: Long,
  ): WorkloadApiQueuePoller =
    WorkloadApiQueuePoller(
      workloadApiClient,
      metricClient,
      featureFlagClient,
      pollSizeItems,
      pollIntervalSeconds,
      WorkloadPriority.DEFAULT,
    )
}
