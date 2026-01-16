/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteWorkloadLauncherConfig
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePoller
import io.micronaut.context.annotation.Factory
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
    workloadLauncherConfiguration: AirbyteWorkloadLauncherConfig,
  ): WorkloadApiQueuePoller =
    WorkloadApiQueuePoller(
      workloadApiClient,
      metricClient,
      featureFlagClient,
      workloadLauncherConfiguration.consumer.highPriorityQueue.pollSizeItems,
      workloadLauncherConfiguration.consumer.highPriorityQueue.pollIntervalSeconds
        .toLong(),
      WorkloadPriority.HIGH,
    )

  @Singleton
  @Named("defaultPriorityQueuePoller")
  fun defaultPriorityQueuePoller(
    workloadApiClient: WorkloadApiClient,
    metricClient: MetricClient,
    featureFlagClient: FeatureFlagClient,
    workloadLauncherConfiguration: AirbyteWorkloadLauncherConfig,
  ): WorkloadApiQueuePoller =
    WorkloadApiQueuePoller(
      workloadApiClient,
      metricClient,
      featureFlagClient,
      workloadLauncherConfiguration.consumer.defaultQueue.pollSizeItems,
      workloadLauncherConfiguration.consumer.defaultQueue.pollIntervalSeconds
        .toLong(),
      WorkloadPriority.DEFAULT,
    )
}
