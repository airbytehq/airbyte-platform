package io.airbyte.workload.launcher.pods

import io.airbyte.commons.json.Jsons
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.process.AsyncOrchestratorPodProcess.KUBE_POD_INFO
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.POD_NAME_PREFIX
import io.airbyte.workers.sync.ReplicationLauncherWorker.REPLICATION
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClientException
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class KubePodClient(
  private val orchestratorLauncher: OrchestratorPodLauncher,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("orchestratorKubeContainerInfo") private val kubeContainerInfo: KubeContainerInfo,
  @Named("orchestratorEnvMap") private val envMap: Map<String, String>,
  @Named("orchestratorNodeSelectors") private val nodeSelectors: Map<String, String>,
  @Named("orchestratorCustomNodeSelectors") private val customNodeSelectors: Map<String, String>,
) {
  object Constants {
    const val WORKLOAD_ID = "workload_id"
  }

  fun podsExistForWorkload(workloadId: String): Boolean {
    return orchestratorLauncher.podsExistForLabels(mapOf(Pair(WORKLOAD_ID, workloadId)))
  }

  fun launchReplication(
    input: ReplicationInput,
    workloadId: String,
  ) {
    val labels = mapOf(Pair(WORKLOAD_ID, workloadId))

    val podName = getPodName(input.jobRunConfig.jobId, input.jobRunConfig.attemptId)

    val kubePodInfo =
      KubePodInfo(
        namespace,
        podName,
        kubeContainerInfo,
      )

    val pod: Pod
    try {
      pod =
        orchestratorLauncher.create(
          labels,
          input.syncResourceRequirements?.orchestrator,
          getNodeSelectors(input),
          kubePodInfo,
        )
    } catch (e: KubernetesClientException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw WorkerException(
        "Failed to create pod $podName.",
        e,
      )
    }

    val fileMap = buildFileMap(input, input.jobRunConfig, kubePodInfo)

    try {
      orchestratorLauncher.copyFilesToKubeConfigVolumeMain(pod, fileMap)
    } catch (e: Exception) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw WorkerException(
        "Failed to copy files to pod $podName.",
        e,
      )
    }
  }

  private fun getPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$POD_NAME_PREFIX-job-$jobId-attempt-$attemptId"
  }

  private fun getNodeSelectors(input: ReplicationInput): Map<String, String> {
    val isCustom = input.sourceLauncherConfig.isCustomConnector || input.destinationLauncherConfig.isCustomConnector

    return if (isCustom) customNodeSelectors else nodeSelectors
  }

  private fun buildFileMap(
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_ENV_MAP to Jsons.serialize(envMap),
      OrchestratorConstants.INIT_FILE_APPLICATION to REPLICATION,
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to Jsons.serialize(jobRunConfig),
      OrchestratorConstants.INIT_FILE_INPUT to Jsons.serialize(input),
      INIT_FILE_SOURCE_LAUNCHER_CONFIG to Jsons.serialize(input.sourceLauncherConfig),
      INIT_FILE_DESTINATION_LAUNCHER_CONFIG to Jsons.serialize(input.destinationLauncherConfig),
      KUBE_POD_INFO to Jsons.serialize(kubePodInfo),
    )
  }
}
