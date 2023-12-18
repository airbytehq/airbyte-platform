/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import datadog.trace.api.Trace
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.KUBERNETES_RESOURCE_MONITOR_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RUNNING_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STATUS_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STOPPED_STATUS
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant

private val logger = KotlinLogging.logger {}

/**
 * Monitors for resource exhaustion in the Kubernetes cluster.
 */
@Singleton
open class KubeResourceMonitor(
  private val kubernetesClient: KubernetesClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
  @Value("\${airbyte.kubernetes.pending-time-limit-sec}") private val pendingTimeLimitSec: Long,
  private val customMetricPublisher: CustomMetricPublisher,
  private val workerFactory: WorkerFactory,
) {
  var isPollingSuspended: Boolean = false

  /**
   * Checks for pods in the configured namespace that have been in a pending
   * state for longer than the allowed pending time limit.  The goal of this
   * check is to determine if the cluster has run out of resources and is
   * unable to run new pods.
   */
  @Trace(operationName = KUBERNETES_RESOURCE_MONITOR_NAME)
  @Scheduled(fixedRate = "\${airbyte.kubernetes.resource-check-rate}")
  @Instrument(
    start = "WORKLOAD_LAUNCHER_KUBERNETES_RESOURCE_MONITOR_START",
    duration = "WORKLOAD_LAUNCHER_KUBERNETES_RESOURCE_MONITOR_RUN",
  )
  open fun checkKubernetesResources() {
    logger.debug { "Scanning pending pods for any older than $pendingTimeLimitSec seconds..." }

    val pendingPods: PodList
    try {
      pendingPods =
        kubernetesClient.pods()
          .inNamespace(namespace)
          .withField(STATUS_PHASE, PENDING)
          .list()
    } catch (e: Exception) {
      logger.info { "Pausing the job polling because the kube API is not responsive" }
      workerFactory.suspendPolling()
      isPollingSuspended = true
      return
    }

    logger.debug { "Found ${pendingPods.items.size} pending pods in the $namespace namespace..." }

    customMetricPublisher.gauge(
      WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS,
      pendingPods,
      { pendingPods.items.size.toDouble() },
    )

    val minPodPendingStartTime = pendingPods.items.minOfOrNull(this::selectLastTransitionTime)
    val pendingDurationSeconds = if (minPodPendingStartTime != null) Duration.between(Instant.now(), minPodPendingStartTime).abs().seconds else 0

    logger.debug { "Oldest pending pod has been pending for $pendingDurationSeconds seconds." }

    if (pendingDurationSeconds > pendingTimeLimitSec) {
      logger.info { "At least one pod has been in a pending state for $pendingDurationSeconds seconds." }
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME,
        pendingDurationSeconds,
        { pendingDurationSeconds.toDouble() },
      )
      if (!isPollingSuspended) {
        logger.info { "Pausing the job polling because pods have been pending for too long" }
        workerFactory.suspendPolling()
        isPollingSuspended = true
      }
    } else {
      logger.info { "No pods have been pending for longer than $pendingTimeLimitSec seconds." }

      if (isPollingSuspended) {
        logger.info { "Resuming polling" }
        workerFactory.resumePolling()
        isPollingSuspended = false
      }
    }
    customMetricPublisher.count(
      WorkloadLauncherMetricMetadata.WORKLOAD_LAUNCHER_POLLER_STATUS,
      MetricAttribute(STATUS_TAG, if (isPollingSuspended) STOPPED_STATUS else RUNNING_STATUS),
    )
  }

  private fun selectLastTransitionTime(p: Pod): Instant {
    if (p.status.conditions.size == 1) {
      return Instant.parse(p.status.conditions.first().lastTransitionTime)
    } else {
      return Instant.now()
    }
  }

  companion object {
    const val PENDING = "Pending"
    const val STATUS_PHASE = "status.phase"
  }
}
