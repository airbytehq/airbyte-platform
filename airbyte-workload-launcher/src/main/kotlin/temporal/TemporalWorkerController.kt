/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.WorkloadLauncherConsumerEnabled
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RUNNING_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STOPPED_STATUS
import io.micronaut.context.annotation.Property
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean

@Singleton
class TemporalWorkerController(
  @Property(name = "airbyte.workload-launcher.geography") private val geography: String,
  @Property(name = "airbyte.data-plane-name") private val dataPlaneName: String,
  @Named("workloadLauncherQueue") private val launcherQueue: String,
  @Named("workloadLauncherHighPriorityQueue") private val launcherHighPriorityQueue: String,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
  private val temporalLauncherWorker: TemporalLauncherWorker,
) {
  private val started: AtomicBoolean = AtomicBoolean(false)

  fun start() {
    started.set(true)
    temporalLauncherWorker.initialize(launcherQueue, launcherHighPriorityQueue)
    checkWorkerStatus()
  }

  @Scheduled(fixedRate = "PT10S")
  fun checkWorkerStatus() {
    if (started.get()) {
      val context = Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
      val shouldRun = featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, context)
      if (shouldRun) {
        temporalLauncherWorker.resumePolling()
      } else {
        temporalLauncherWorker.suspendPolling()
      }
    }
  }

  @Scheduled(fixedRate = "PT60S")
  fun reportPollerStatuses() {
    reportPollerStatus(launcherQueue)
    reportPollerStatus(launcherHighPriorityQueue)
  }

  private fun reportPollerStatus(queueName: String) {
    val isPollingSuspended = temporalLauncherWorker.isSuspended(queueName)
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_POLLER_STATUS,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.QUEUE_NAME_TAG, queueName),
          MetricAttribute(MetricTags.STATUS_TAG, if (isPollingSuspended) STOPPED_STATUS else RUNNING_STATUS),
        ),
    )
  }
}
