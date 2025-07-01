/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_COMPLETED_VALUE
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_RUNNING_VALUE
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_KEY
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_VALUE
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

/**
 * A class that sweeps pods (labeled airbyte=job-pod) older than certain TTLs.
 *
 * @param client Fabric8 KubernetesClient
 * @param namespace The Kubernetes namespace to search in (defaults to "default")
 * @param runningTtl If non-null, running pods older than now - runningTtl will be deleted
 * @param succeededTtl If non-null, succeeded pods older than now - succeededTtl will be deleted
 * @param unsuccessfulTtl If non-null, failed/unknown pods older than now - unsuccessfulTtl will be deleted
 */
private val logger = KotlinLogging.logger {}
private const val DELETE_TIMEOUT_MINUTES = 1L

@Singleton
class PodSweeper(
  private val kubernetesClient: KubernetesClient,
  private val metricClient: MetricClient,
  private val clock: Clock,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
  @Named("kubernetesClientRetryPolicy") private val kubernetesClientRetryPolicy: RetryPolicy<Any>,
  @Value("\${airbyte.pod-sweeper.runningTtl}") private val runningTtl: Long? = null,
  @Value("\${airbyte.pod-sweeper.succeededTtl}") private val succeededTtl: Long? = null,
  @Value("\${airbyte.pod-sweeper.unsuccessfulTtl}") private val unsuccessfulTtl: Long? = null,
) {
  @Scheduled(fixedRate = "\${airbyte.pod-sweeper.rate}")
  fun sweepPods() {
    logger.info { "Starting pod sweeper cycle in namespace [$namespace]..." }

    val now = Instant.ofEpochMilli(clock.millis())
    val runningCutoff = runningTtl?.minutes?.let { now.minus(it.toJavaDuration()) }
    val succeededCutoff = succeededTtl?.minutes?.let { now.minus(it.toJavaDuration()) }
    val unsuccessfulCutoff = unsuccessfulTtl?.minutes?.let { now.minus(it.toJavaDuration()) }

    runningCutoff?.let {
      logger.info { "Will sweep Running pods older than $it (UTC)." }
    }
    succeededCutoff?.let {
      logger.info { "Will sweep Succeeded pods older than $it (UTC)." }
    }
    unsuccessfulCutoff?.let {
      logger.info { "Will sweep unsuccessful pods older than $it (UTC)." }
    }

    // List pods labeled 'airbyte=job-pod'
    val podList: PodList =
      kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withLabel(SWEEPER_LABEL_KEY, SWEEPER_LABEL_VALUE)
        .list()

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
    pod.metadata?.let {
      it.name?.let { name ->
        logger.info { "Deleting pod [$name]. Reason: $reason" }
        runKubeCommand {
          try {
            kubernetesClient
              .pods()
              .inNamespace(namespace)
              .resource(pod)
              .withTimeout(DELETE_TIMEOUT_MINUTES, TimeUnit.MINUTES)
              .delete()
          } catch (e: Exception) {
            // If we time out (or otherwise fail), force-delete immediately
            logger.warn(e) {
              "Timed out or error encountered while deleting pod [$name] in namespace [$namespace]. " +
                "Forcing immediate deletion with gracePeriod=0."
            }

            kubernetesClient
              .pods()
              .inNamespace(namespace)
              .resource(pod)
              .withGracePeriod(0)
              .delete()
          }
        }
        metricClient.count(metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_POD_SWEEPER_COUNT, attributes = arrayOf(MetricAttribute("phase", phase)))
      }
    }
  }

  private fun <T> runKubeCommand(kubeCommand: () -> T) {
    try {
      Failsafe.with(kubernetesClientRetryPolicy).get(
        CheckedSupplier { kubeCommand() },
      )
    } catch (e: Exception) {
      logger.error(e) { "Could not delete the pod" }
    }
  }
}
