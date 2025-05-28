/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workload.launcher.constants.ContainerConstants
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.FABRIC8_COMPLETED_REASON_VALUE
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_COMPLETED_VALUE
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_PHASE_FIELD_NAME
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.MAX_DELETION_TIMEOUT
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.VALID_INIT_CONTAINER_EXIT_CODES
import io.fabric8.kubernetes.api.model.ContainerState
import io.fabric8.kubernetes.api.model.DeletionPropagation
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.api.model.StatusDetails
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.readiness.Readiness
import io.fabric8.kubernetes.client.utils.PodStatusUtil
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

/**
 * Atomic operations on the raw Kube api. Domain level information should be opaque to this layer.
 */
@Singleton
class KubePodLauncher(
  private val kubernetesClient: KubernetesClient,
  private val metricClient: MetricClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("kubernetesClientRetryPolicy") private val kubernetesClientRetryPolicy: RetryPolicy<Any>,
) {
  fun create(pod: Pod): Pod =
    runKubeCommand(
      {
        kubernetesClient
          .pods()
          .inNamespace(namespace)
          .resource(pod)
          .serverSideApply()
      },
      "pod_create",
    )

  fun waitForPodInitStartup(
    pod: Pod,
    waitDuration: Duration,
  ) = waitForPodInit(pod, waitDuration)

  fun waitForPodInitComplete(
    pod: Pod,
    waitDuration: Duration,
  ) {
    val initializedPod =
      runKubeCommand(
        {
          kubernetesClient
            .resource(pod)
            .waitUntilCondition(
              { p: Pod? ->
                p?.let {
                  p.status.initContainerStatuses.isNotEmpty() &&
                    p.status.initContainerStatuses[0]
                      .state.terminated != null
                } ?: false
              },
              waitDuration.toMinutes(),
              TimeUnit.MINUTES,
            )
        },
        "wait",
      )

    val containerState =
      initializedPod
        .status
        .initContainerStatuses[0]
        .state

    if (containerState.terminated == null) {
      throw TimeoutException(
        "Init container for Pod: ${pod.fullResourceName} was not in terminated state after: ${waitDuration.toMinutes()} ${TimeUnit.MINUTES}. " +
          "Actual container state: $containerState.",
      )
    }

    val initContainerExitCode =
      initializedPod.status.initContainerStatuses[0]
        .state.terminated.exitCode

    val terminationReason =
      initializedPod
        .status
        .initContainerStatuses[0]
        .state
        .terminated
        .reason

    if (terminationReason != FABRIC8_COMPLETED_REASON_VALUE && !VALID_INIT_CONTAINER_EXIT_CODES.contains(initContainerExitCode)) {
      throw RuntimeException(
        "Init container for Pod: ${pod.fullResourceName} did not complete successfully. " +
          "Actual termination reason: $terminationReason.",
      )
    }
  }

  private fun waitForPodInit(
    pod: Pod,
    waitDuration: Duration,
  ) {
    val initializedPod =
      runKubeCommand(
        {
          kubernetesClient
            .resource(pod)
            .waitUntilCondition(
              { p: Pod ->
                (
                  p.status.initContainerStatuses.isNotEmpty() &&
                    p.status.initContainerStatuses[0]
                      .state.waiting == null
                )
              },
              waitDuration.toMinutes(),
              TimeUnit.MINUTES,
            )
        },
        "wait",
      )

    val containerState: ContainerState =
      initializedPod
        .status
        .initContainerStatuses[0]
        .state

    if (containerState.running == null) {
      throw RuntimeException(
        "Init container for Pod: ${pod.fullResourceName} was not in a running state after: ${waitDuration.toMinutes()} ${TimeUnit.MINUTES}. " +
          "Actual container state: $containerState.",
      )
    }
  }

  fun waitForPodReadyOrTerminal(
    labels: Map<String, String>,
    waitDuration: Duration,
  ) {
    runKubeCommand(
      {
        kubernetesClient
          .pods()
          .inNamespace(namespace)
          .withLabels(labels)
          .waitUntilCondition(
            { p: Pod? ->
              Objects.nonNull(p) &&
                (Readiness.getInstance().isReady(p) || isTerminal(p))
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )
  }

  fun waitForPodReadyOrTerminalByPod(
    pod: Pod,
    waitDuration: Duration,
  ) {
    runKubeCommand(
      {
        kubernetesClient
          .resource(pod)
          .waitUntilCondition(
            { p: Pod? ->
              Objects.nonNull(p) &&
                (Readiness.getInstance().isReady(p) || isTerminal(p))
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )
  }

  fun podsRunning(labels: Map<String, String>): Boolean {
    try {
      return runKubeCommand(
        {
          kubernetesClient
            .pods()
            .inNamespace(namespace)
            .withLabels(labels)
            .list()
            .items
            .stream()
            .filter { kubePod: Pod -> !isTerminal(kubePod) && !PodStatusUtil.isInitializing(kubePod) }
            .findAny()
            .isPresent
        },
        "list",
      )
    } catch (e: Exception) {
      logger.warn(e) { "Could not find pods running for $labels, presuming no pods are running" }
      return false
    }
  }

  fun deleteActivePods(labels: Map<String, String>): List<StatusDetails> {
    return runKubeCommand(
      {
        val statuses =
          listActivePods(labels)
            .list()
            .items
            .flatMap { p ->
              kubernetesClient
                .pods()
                .inNamespace(namespace)
                .resource(p)
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete()
            }

        if (statuses.isEmpty()) {
          return@runKubeCommand statuses
        }

        listActivePods(labels)
          .waitUntilCondition(Objects::isNull, MAX_DELETION_TIMEOUT, TimeUnit.SECONDS)

        statuses
      },
      "delete",
    )
  }

  /**
   * Checks that the pod's main container(s) are in a terminal state.
   */
  private fun isTerminal(pod: Pod?): Boolean {
    // if pod is null or there is no status default to false.
    if (pod?.status?.initContainerStatuses == null) {
      return false
    }

    val hasInitContainerStatus = pod.status.initContainerStatuses.size > 0
    if (!hasInitContainerStatus) {
      return false
    }

    val initContainerExitCode =
      pod.status.initContainerStatuses[0]
        ?.state
        ?.terminated
        ?.exitCode
    // we are certainly not terminal if the init container hasn't exited
    if (initContainerExitCode == null) {
      return false
    }

    // Edge case of the init container exiting with specific error codes
    // Those are configuration related errors that cause the main container to never start however, we did not fail
    // because the launch was a success from a workload-infra pov
    if (initContainerExitCode == InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE
    ) {
      return true
    }

    // Get statuses for all "non-init" containers.
    val mainContainerStatuses =
      pod.status
        .containerStatuses
        .stream()
        .filter { containerStatus -> (ContainerConstants.INIT_CONTAINER_NAME) != containerStatus.name }
        .toList()

    // There should be at least 1 container with a status.
    if (mainContainerStatuses.size < 1) {
      logger.warn { "Unexpectedly no non-init container statuses found for pod: ${pod.fullResourceName}" }
      return false
    }

    return mainContainerStatuses.all {
      it.state?.terminated != null
    }
  }

  private fun listActivePods(labels: Map<String, String>): FilterWatchListDeletable<Pod, PodList, PodResource> {
    return kubernetesClient
      .pods()
      .inNamespace(namespace)
      .withLabels(labels)
      .withoutField(KUBECTL_PHASE_FIELD_NAME, KUBECTL_COMPLETED_VALUE) // filters out completed pods
  }

  private fun <T> runKubeCommand(
    kubeCommand: () -> T,
    commandName: String,
  ): T {
    try {
      return Failsafe.with(kubernetesClientRetryPolicy).get(
        object : CheckedSupplier<T> {
          override fun get(): T = kubeCommand()
        },
      )
    } catch (e: Exception) {
      val attributes: List<MetricAttribute> = listOf(MetricAttribute("operation", commandName))
      val attributesArray = attributes.toTypedArray<MetricAttribute>()
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_ERROR, attributes = attributesArray)

      throw e
    }
  }

  object Constants {
    // Wait why is this named like this?
    // Explanation: Kubectl displays "Completed" but the selector expects "Succeeded"
    const val KUBECTL_COMPLETED_VALUE = "Succeeded"
    const val KUBECTL_RUNNING_VALUE = "Running"

    // Explanation: Unlike Kubectl, Fabric8 shows and uses "Completed" for termination reasons
    const val FABRIC8_COMPLETED_REASON_VALUE = "Completed"
    const val KUBECTL_PHASE_FIELD_NAME = "status.phase"
    const val MAX_DELETION_TIMEOUT = 45L

    val VALID_INIT_CONTAINER_EXIT_CODES =
      setOf(
        InitContainerConstants.SUCCESS_EXIT_CODE,
        InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE,
      )
  }
}
