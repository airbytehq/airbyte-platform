package io.airbyte.workload.launcher.temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.WorkloadLauncherConsumerEnabled
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.QUEUE_NAME_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RUNNING_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STATUS_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STOPPED_STATUS
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.micronaut.context.annotation.Property
import io.micronaut.scheduling.annotation.Scheduled
import io.temporal.worker.WorkerFactory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class TemporalWorkerController(
  @Property(name = "airbyte.workload-launcher.geography") private val geography: String,
  @Property(name = "airbyte.data-plane-name") private val dataPlaneName: String,
  @Named("workloadLauncherQueue") private val launcherQueue: String,
  @Named("workloadLauncherHighPriorityQueue") private val launcherHighPriorityQueue: String,
  private val customMetricPublisher: CustomMetricPublisher,
  private val featureFlagClient: FeatureFlagClient,
  @Named("workerFactory") private val workerFactory: WorkerFactory,
  @Named("highPriorityWorkerFactory") private val highPriorityWorkerFactory: WorkerFactory,
) {
  @Scheduled(fixedRate = "PT10S")
  fun checkWorkerStatus() {
    val context = Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
    val shouldRun = featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, context)
    if (shouldRun) {
      workerFactory.resumePolling()
      highPriorityWorkerFactory.resumePolling()
    } else {
      workerFactory.suspendPolling()
      highPriorityWorkerFactory.suspendPolling()
    }
  }

  @Scheduled(fixedRate = "PT60S")
  fun reportPollerStatuses() {
    reportPollerStatus(workerFactory, launcherQueue)
    reportPollerStatus(highPriorityWorkerFactory, launcherHighPriorityQueue)
  }

  private fun reportPollerStatus(
    wf: WorkerFactory,
    queueName: String,
  ) {
    val isPollingSuspended = wf.getWorker(queueName).isSuspended
    customMetricPublisher.count(
      WorkloadLauncherMetricMetadata.WORKLOAD_LAUNCHER_POLLER_STATUS,
      MetricAttribute(QUEUE_NAME_TAG, queueName),
      MetricAttribute(STATUS_TAG, if (isPollingSuspended) STOPPED_STATUS else RUNNING_STATUS),
    )
  }
}
