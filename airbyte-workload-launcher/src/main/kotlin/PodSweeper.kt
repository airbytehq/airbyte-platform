/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbytePodSweeperConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.micronaut.runtime.POD_SWEEPER_PREFIX
import io.airbyte.workload.launcher.client.KubernetesClientWrapper
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_COMPLETED_VALUE
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_RUNNING_VALUE
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

/**
 * A class that sweeps pods (labeled airbyte=job-pod) older than certain TTLs.  This class
 * is marked open to support @Instrument AOP.
 *
 * @param k8sWrapper The wrapped Fabric8 KubernetesClient
 * @param metricClient The [MetricClient]
 * @param clock The [Clock] used to determine what to sweep
 * @param airbyteWorkerConfig The [AirbyteWorkerConfig] that contains the K8s cluster namespace
 * @param airbytePodSweeperConfig The [AirbytePodSweeperConfig] that contains the TTLs for sweeping
 */
@Singleton
open class PodSweeper(
  private val k8sWrapper: KubernetesClientWrapper,
  private val metricClient: MetricClient,
  private val clock: Clock,
  private val airbyteWorkerConfig: AirbyteWorkerConfig,
  private val airbytePodSweeperConfig: AirbytePodSweeperConfig,
) {
  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "pod_sweeper")],
  )
  @Scheduled(fixedRate = "\${$POD_SWEEPER_PREFIX.rate}")
  open fun sweepPods() {
    logger.info { "Starting pod sweeper cycle in namespace [${airbyteWorkerConfig.job.kubernetes.namespace}]..." }

    val now = Instant.ofEpochMilli(clock.millis())
    val runningCutoff =
      if (airbytePodSweeperConfig.runningTtl >
        0
      ) {
        airbytePodSweeperConfig.runningTtl.minutes.let { now.minus(it.toJavaDuration()) }
      } else {
        null
      }
    val succeededCutoff =
      if (airbytePodSweeperConfig.succeededTtl >
        0
      ) {
        airbytePodSweeperConfig.succeededTtl.minutes.let { now.minus(it.toJavaDuration()) }
      } else {
        null
      }
    val unsuccessfulCutoff =
      if (airbytePodSweeperConfig.unsuccessfulTtl >
        0
      ) {
        airbytePodSweeperConfig.unsuccessfulTtl.minutes.let { now.minus(it.toJavaDuration()) }
      } else {
        null
      }

    runningCutoff?.let {
      logger.info { "Will sweep Running pods older than $it (UTC)." }
    }
    succeededCutoff?.let {
      logger.info { "Will sweep Succeeded pods older than $it (UTC)." }
    }
    unsuccessfulCutoff?.let {
      logger.info { "Will sweep unsuccessful pods older than $it (UTC)." }
    }

    val podList: PodList = k8sWrapper.listJobPods(airbyteWorkerConfig.job.kubernetes.namespace)

    for (pod in podList.items) {
      val phase = pod.status?.phase
      if (phase == null) {
        // If there's no status or phase, skip
        continue
      }

      // Compute the "most relevant" date to compare:
      // by default: PodCondition[0].lastTransitionTime if present, else use Pod.status.startTime
      val transitionTime =
        pod.status
          ?.conditions
          ?.firstOrNull() // conditions[0] if it exists
          ?.lastTransitionTime
      val startTime = pod.status?.startTime

      // We fallback: if transitionTime is non-null, use that; otherwise, use startTime.
      // If both are null, skip (can't apply TTL).
      val podInstant: Instant? =
        when {
          !transitionTime.isNullOrBlank() -> parseKubeDate(transitionTime)
          !startTime.isNullOrBlank() -> parseKubeDate(startTime)
          else -> null
        }

      if (podInstant == null) {
        // Could not parse any valid date, skip
        logger.info { "Skipping pod ${pod.metadata?.name} - no valid transition/start time found." }
        continue
      }

      // Compare the phase & TTL cutoffs
      when (phase) {
        KUBECTL_RUNNING_VALUE -> {
          if (runningCutoff != null && podInstant.isBefore(runningCutoff)) {
            deletePod(pod, phase, "Running since $podInstant")
          }
        }

        KUBECTL_COMPLETED_VALUE -> {
          if (succeededCutoff != null && podInstant.isBefore(succeededCutoff)) {
            deletePod(pod, phase, "Succeeded since $podInstant")
          }
        }

        else -> {
          // "Failed", "Unknown", "Pending", etc.
          if (unsuccessfulCutoff != null && podInstant.isBefore(unsuccessfulCutoff)) {
            deletePod(pod, phase, "Unsuccessful ($phase) since $podInstant")
          }
        }
      }
    }
    logger.info { "Completed pod sweeper cycle." }
  }

  /**
   * Parses an ISO8601 date/time string from Kubernetes fields (e.g., "2025-01-15T10:00:00Z")
   */
  private fun parseKubeDate(dateStr: String): Instant? =
    try {
      Instant.parse(dateStr)
    } catch (e: Exception) {
      logger.error(e) { "Error parsing date [$dateStr]" }
      null
    }

  /**
   * Actually deletes the given pod from the cluster.
   */
  private fun deletePod(
    pod: Pod,
    phase: String,
    reason: String,
  ) {
    if (k8sWrapper.deletePod(pod = pod, namespace = airbyteWorkerConfig.job.kubernetes.namespace, reason = reason)) {
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_POD_SWEEPER_COUNT, attributes = arrayOf(MetricAttribute("phase", phase)))
    }
  }
}
