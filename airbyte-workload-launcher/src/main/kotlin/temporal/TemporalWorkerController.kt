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
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueueConsumer
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
  private val workloadApiQueueConsumer: WorkloadApiQueueConsumer,
  private val temporalQueueConsumer: TemporalLauncherWorker,
) : ApplicationEventListener<DataplaneConfig> {
  private val started: AtomicBoolean = AtomicBoolean(false)
  private var currentDataplaneConfig: DataplaneConfig? = null
  private var pollersConsuming: Boolean? = null
  private var temporalPollersConsuming: Boolean? = null

  fun start() {
    started.set(true)

    if (useDataplaneAuthNFlow()) {
      updateEnabledStatus()
    } else {
      workloadApiQueueConsumer.initialize(launcherQueue, launcherHighPriorityQueue)
      temporalQueueConsumer.initialize(launcherQueue, launcherHighPriorityQueue)
      checkWorkerStatus()
    }
  }

  @Deprecated("This will be replaced by DataplaneIdentityService")
  @Scheduled(fixedRate = "PT10S")
  fun checkWorkerStatus() {
    if (useDataplaneAuthNFlow()) {
      return
    }

    if (started.get()) {
      val context = Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
      val shouldRun = featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, context)
      if (shouldRun) {
        workloadApiQueueConsumer.resumePolling()
        temporalQueueConsumer.resumePolling()
      } else {
        workloadApiQueueConsumer.suspendPolling()
        temporalQueueConsumer.suspendPolling()
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
    val isPollingSuspended = temporalQueueConsumer.isSuspended(queueName)
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
      workloadApiQueueConsumer.initialize(event.dataplaneGroupName, event.dataplaneGroupName)
      temporalQueueConsumer.initialize(launcherQueue, launcherHighPriorityQueue)
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
        workloadApiQueueConsumer.resumePolling()
      } else {
        workloadApiQueueConsumer.suspendPolling()
      }
      pollersConsuming = shouldPollerConsume
    }

    val shouldTemporalPollersConsume = shouldPollerConsume && currentDataplaneConfig?.temporalConsumerEnabled ?: false
    if (shouldTemporalPollersConsume != temporalPollersConsuming) {
      if (shouldTemporalPollersConsume) {
        temporalQueueConsumer.resumePolling()
      } else {
        temporalQueueConsumer.suspendPolling()
      }
      temporalPollersConsuming = shouldTemporalPollersConsume
    }
  }
}
