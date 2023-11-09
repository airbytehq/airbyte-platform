package io.airbyte.workload.launcher.pods

import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.model.setDestinationLabels
import io.airbyte.workload.launcher.model.setSourceLabels
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import io.fabric8.kubernetes.api.model.Pod
import jakarta.inject.Singleton
import java.lang.RuntimeException

/**
 * Interface layer between domain and Kube layers.
 * Composes raw Kube layer atomic operations to perform business operations.
 */
@Singleton
class KubePodClient(
  private val orchestratorLauncher: OrchestratorPodLauncher,
  private val labeler: PodLabeler,
  private val mapper: PayloadKubeInputMapper,
) {
  object Constants {
    const val WORKLOAD_ID = "workload_id"
    const val MUTEX_KEY = "mutex_key"
  }

  fun podsExistForWorkload(workloadId: String): Boolean {
    return orchestratorLauncher.podsExist(mapOf(Pair(WORKLOAD_ID, workloadId)))
  }

  fun launchReplication(
    input: ReplicationInput,
    workloadId: String,
  ) {
    val passThroughLabels = labeler.getSharedLabels(input, workloadId)

    val inputWithLabels =
      input
        .setSourceLabels(passThroughLabels)
        .setDestinationLabels(passThroughLabels)

    val kubeInput = mapper.toKubeInput(inputWithLabels, workloadId)

    val pod: Pod
    try {
      pod =
        orchestratorLauncher.create(
          kubeInput.orchestratorLabels,
          kubeInput.resourceReqs,
          kubeInput.nodeSelectors,
          kubeInput.kubePodInfo,
        )
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodsWithLabels(kubeInput.orchestratorLabels)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Orchestrator pod failed to start within allotted timeout.",
        e,
      )
    }

    try {
      orchestratorLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to copy files to orchestrator pod ${kubeInput.kubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodsWithLabels(kubeInput.sourceLabels)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Source pod failed to start within allotted timeout.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodsWithLabels(kubeInput.destinationLabels)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Destination pod failed to start within allotted timeout.",
        e,
      )
    }
  }

  fun deleteMutexPods(input: ReplicationInput) {
    val labels = labeler.getMutexLabels(input)

    orchestratorLauncher.deletePods(labels)
  }

  fun deleteWorkload(workloadId: String) {
    val labels = labeler.getWorkloadLabels(workloadId)

    orchestratorLauncher.deletePods(labels)
  }
}
