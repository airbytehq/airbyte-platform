/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_KEY
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.SWEEPER_LABEL_VALUE
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import io.fabric8.kubernetes.client.dsl.PodResource
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

private const val DELETE_TIMEOUT_MINUTES = 1L

typealias PodFluentFilter = FilterWatchListDeletable<Pod, PodList, PodResource>

@Singleton
class KubernetesClientWrapper(
  val kubernetesClient: KubernetesClient,
  @Named("kubernetesClientRetryPolicy") private val kubernetesClientRetryPolicy: RetryPolicy<Any>,
) {
  /**
   * List pods labeled 'airbyte=job-pod'
   */
  fun listJobPods(
    namespace: String,
    extraFilters: ((PodFluentFilter) -> PodFluentFilter)? = null,
  ): PodList {
    val filterable =
      kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withLabel(SWEEPER_LABEL_KEY, SWEEPER_LABEL_VALUE)

    return (extraFilters?.let { it(filterable) } ?: filterable).list()
  }

  /** Adds labels to the given pod. */
  fun addLabelsToPod(
    pod: Pod,
    labels: Map<String, String>,
  ) {
    kubernetesClient
      .pods()
      .inNamespace(pod.metadata.namespace)
      .withName(pod.metadata.name)
      .edit { podSpec ->
        podSpec
          .edit()
          .editMetadata()
          .addToLabels(labels)
          .endMetadata()
          .build()
      }
  }

  /**
   * Deletes the given pod from the cluster.
   *
   * Returns true if we were able to run the delete command successfully.
   */
  fun deletePod(
    pod: Pod,
    namespace: String,
    reason: String,
  ): Boolean =
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
      }
    } ?: false

  private fun <T> runKubeCommand(kubeCommand: () -> T): Boolean {
    try {
      Failsafe.with(kubernetesClientRetryPolicy).get(
        CheckedSupplier { kubeCommand() },
      )
      return true
    } catch (e: Exception) {
      logger.error(e) { "Could not run the kubernetes command" }
    }
    return false
  }
}
