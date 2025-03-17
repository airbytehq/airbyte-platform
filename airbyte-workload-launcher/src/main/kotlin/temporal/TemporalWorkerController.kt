/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.WorkloadLauncherConsumerEnabled
import io.airbyte.featureflag.WorkloadLauncherUseDataPlaneAuthNFlow
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RUNNING_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STOPPED_STATUS
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventListener
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
//  private val pollingConsumer: WorkloadApiQueueConsumer, // Uncomment and comment the line below to swap consumers. TODO: wire this up properly.
  private val pollingConsumer: TemporalLauncherWorker,
) : ApplicationEventListener<DataplaneConfig> {
  private val started: AtomicBoolean = AtomicBoolean(false)
  private var currentDataplaneConfig: DataplaneConfig? = null
  private var pollersConsuming: Boolean? = null

  fun start() {
    started.set(true)

    if (useDataplaneAuthNFlow()) {
      updateEnabledStatus()
    } else {
      pollingConsumer.initialize(launcherQueue, launcherHighPriorityQueue)
      checkWorkerStatus()
    }
  }

  @Deprecated("This will be replaced by ControlplanePoller")
  @Scheduled(fixedRate = "PT10S")
  fun checkWorkerStatus() {
    if (useDataplaneAuthNFlow()) {
      return
    }

    if (started.get()) {
      val context = Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
      val shouldRun = featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, context)
      if (shouldRun) {
        pollingConsumer.resumePolling()
      } else {
        pollingConsumer.suspendPolling()
      }
    }
  }

  @Scheduled(fixedRate = "PT60S")
  fun reportPollerStatuses() {
    reportPollerStatus(launcherQueue)
    reportPollerStatus(launcherHighPriorityQueue)
  }

  private fun useDataplaneAuthNFlow(): Boolean = featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, PlaneName(dataPlaneName))

  private fun reportPollerStatus(queueName: String) {
    val isPollingSuspended = pollingConsumer.isSuspended(queueName)
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_POLLER_STATUS,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.QUEUE_NAME_TAG, queueName),
          MetricAttribute(MetricTags.STATUS_TAG, if (isPollingSuspended) STOPPED_STATUS else RUNNING_STATUS),
        ),
    )
  }

  override fun onApplicationEvent(event: DataplaneConfig) {
    if (currentDataplaneConfig == null) {
      pollingConsumer.initialize(launcherQueue, launcherHighPriorityQueue)
    }
    currentDataplaneConfig = event
    updateEnabledStatus()
  }

  private fun updateEnabledStatus() {
    // If the Controller hasn't been started, we shouldn't consume anything yet.
    // The launcher is either initializing or resuming claims.
    // Same if we do not have a current config, there isn't anything to do yet.
    if (!started.get() || currentDataplaneConfig == null) {
      return
    }

    val shouldPollerConsume = currentDataplaneConfig?.dataplaneEnabled ?: false
    if (shouldPollerConsume != pollersConsuming) {
      if (shouldPollerConsume) {
        pollingConsumer.resumePolling()
      } else {
        pollingConsumer.suspendPolling()
      }
      pollersConsuming = shouldPollerConsume
    }
  }
}
