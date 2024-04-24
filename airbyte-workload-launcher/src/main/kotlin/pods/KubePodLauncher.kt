package io.airbyte.workload.launcher.pods

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.UseCustomK8sInitCheck
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.process.KubePodResourceHelper
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_COMPLETED_VALUE
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.KUBECTL_PHASE_FIELD_NAME
import io.airbyte.workload.launcher.pods.KubePodLauncher.Constants.MAX_DELETION_TIMEOUT
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
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

private val logger = KotlinLogging.logger {}

/**
 * Atomic operations on the raw Kube api. Domain level information should be opaque to this layer.
 */
@Singleton
class KubePodLauncher(
  private val kubernetesClient: KubernetesClient,
  private val metricClient: MetricClient,
  private val kubeCopyClient: KubeCopyClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("kubernetesClientRetryPolicy") private val kubernetesClientRetryPolicy: RetryPolicy<Any>,
  private val featureFlagClient: FeatureFlagClient,
  @Property(name = "airbyte.data-plane-name") private val dataPlaneName: String?,
) {
  fun create(pod: Pod): Pod {
    return runKubeCommand(
      {
        kubernetesClient.pods()
          .inNamespace(namespace)
          .resource(pod)
          .serverSideApply()
      },
      "pod_create",
    )
  }

  fun waitForPodInit(
    pod: Pod,
    waitDuration: Duration,
  ) {
    if (shouldUseCustomK8sInitCheck()) {
      return waitForPodInitCustomCheck(pod, waitDuration)
    } else {
      return waitForPodInitDefaultCheck(pod, waitDuration)
    }
  }

  private fun shouldUseCustomK8sInitCheck() =
    dataPlaneName.isNullOrBlank() ||
      featureFlagClient.boolVariation(
        UseCustomK8sInitCheck,
        PlaneName(dataPlaneName),
      )

  private fun waitForPodInitDefaultCheck(
    pod: Pod,
    waitDuration: Duration,
  ) {
    runKubeCommand(
      {
        kubernetesClient
          .resource(pod)
          .waitUntilCondition(
            { p: Pod ->
              PodStatusUtil.isInitializing(p)
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )
  }

  private fun waitForPodInitCustomCheck(
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
                    p.status.initContainerStatuses[0].state.waiting == null
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
        kubernetesClient.pods()
          .inNamespace(namespace)
          .withLabels(labels)
          .waitUntilCondition(
            { p: Pod? ->
              Objects.nonNull(p) &&
                (Readiness.getInstance().isReady(p) || KubePodResourceHelper.isTerminal(p))
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
                (Readiness.getInstance().isReady(p) || KubePodResourceHelper.isTerminal(p))
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )
  }

  fun podsExist(labels: Map<String, String>): Boolean {
    try {
      return runKubeCommand(
        {
          kubernetesClient.pods()
            .inNamespace(namespace)
            .withLabels(labels)
            .list()
            .items
            .stream()
            .filter(
              Predicate<Pod> { kubePod: Pod? ->
                !KubePodResourceHelper.isTerminal(
                  kubePod,
                )
              },
            )
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
              kubernetesClient.pods()
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

  fun copyFilesToKubeConfigVolumeMain(
    pod: Pod,
    files: Map<String, String>,
  ) {
    runKubeCommand(
      {
        kubeCopyClient.copyFilesToKubeConfigVolumeMain(pod, files)
      },
      "kubectl_cp",
    )
  }

  private fun listActivePods(labels: Map<String, String>): FilterWatchListDeletable<Pod, PodList, PodResource> {
    return kubernetesClient.pods()
      .inNamespace(namespace)
      .withLabels(labels)
      .withoutField(KUBECTL_PHASE_FIELD_NAME, KUBECTL_COMPLETED_VALUE) // filters out completed pods
  }

  private fun <T> runKubeCommand(
    kubeCommand: () -> T,
    commandName: String,
  ): T {
    try {
      return Failsafe.with(kubernetesClientRetryPolicy).get { -> kubeCommand() }
    } catch (e: Exception) {
      val attributes: List<MetricAttribute> = listOf(MetricAttribute("operation", commandName))
      val attributesArray = attributes.toTypedArray<MetricAttribute>()
      metricClient.count(OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_ERROR, 1, *attributesArray)

      throw e
    }
  }

  object Constants {
    // Wait why is this named like this?
    // Explanation: Kubectl displays "Completed" but the selector expects "Succeeded"
    const val KUBECTL_COMPLETED_VALUE = "Succeeded"
    const val KUBECTL_PHASE_FIELD_NAME = "status.phase"
    const val MAX_DELETION_TIMEOUT = 45L
  }
}
