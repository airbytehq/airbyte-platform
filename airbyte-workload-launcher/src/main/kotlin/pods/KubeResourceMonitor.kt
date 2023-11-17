/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import datadog.trace.api.Trace
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.KUBERENTES_RESOURCE_MONITOR_NAME
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant

private val logger = KotlinLogging.logger {}

/**
 * Monitors for resource exhaustion in the Kubernetes cluster.
 */
@Singleton
class KubeResourceMonitor(
  private val kubernetesClient: KubernetesClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
  @Value("\${airbyte.kubernetes.pending-time-limit-sec}") private val pendingTimeLimitSec: Long,
  private val customMetricPublisher: CustomMetricPublisher,
) {
  /**
   * Checks for pods in the configured namespace that have been in a pending
   * state for longer than the allowed pending time limit.  The goal of this
   * check is to determine if the cluster has run out of resources and is
   * unable to run new pods.
   */
  @Trace(operationName = KUBERENTES_RESOURCE_MONITOR_NAME)
  @Scheduled(fixedRate = "\${airbyte.kubernetes.resource-check-rate}")
  fun checkKubernetesResources() {
    logger.info { "Scanning pending pods for any older than $pendingTimeLimitSec seconds..." }

    val pendingPods =
      kubernetesClient.pods()
        .inNamespace(namespace)
        .withField(STATUS_PHASE, PENDING)
        .list()

    logger.info { "Found ${pendingPods.items.size} pending pods in the $namespace namespace..." }

    customMetricPublisher.gauge(
      WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS,
      pendingPods,
      { pendingPods.items.size.toDouble() },
    )

    val minPodPendingStartTime = pendingPods.items.minOfOrNull { p -> Instant.parse(p.status.startTime) }
    val pendingDurationSeconds = if (minPodPendingStartTime != null) Duration.between(Instant.now(), minPodPendingStartTime).abs().seconds else 0

    logger.info { "Oldest pending pod has been pending for $pendingDurationSeconds seconds." }

    if (pendingDurationSeconds > pendingTimeLimitSec) {
      logger.info { "At least one pod has been in a pending state for $pendingDurationSeconds seconds." }
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME,
        pendingDurationSeconds,
        { pendingDurationSeconds.toDouble() },
      )
    } else {
      logger.info { "No pods have been pending for longer than $pendingTimeLimitSec seconds." }
    }
  }

  companion object {
    const val PENDING = "Pending"
    const val STATUS_PHASE = "status.phase"
  }
}
