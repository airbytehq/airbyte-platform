package io.airbyte.workload.launcher.client

import io.airbyte.workers.process.KubePodResourceHelper
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.Map
import java.util.function.Predicate

private val LOGGER = KotlinLogging.logger {}

@Singleton
class KubeClient {
  private val kubernetesClient: KubernetesClient = DefaultKubernetesClient()

  fun podsExistForWorkload(
    workloadId: String,
    namespace: String,
  ): Boolean {
    try {
      return kubernetesClient.pods()
        .inNamespace(namespace)
        .withLabels(
          Map.of<String, String>(
            WORKLOAD_ID,
            workloadId,
          ),
        )
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
    } catch (e: Exception) {
      LOGGER.warn(e) { "Could not find pods running for $workloadId, assuming no pods run for the workload" }
      return false
    }
  }
}
