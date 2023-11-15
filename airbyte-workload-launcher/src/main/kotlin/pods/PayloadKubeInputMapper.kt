package io.airbyte.workload.launcher.pods

import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.orchestrator.OrchestratorNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess.KUBE_POD_INFO
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.REPLICATION
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrchestratorResourceReqs
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.airbyte.workload.launcher.serde.ObjectSerializer
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Maps domain layer objects into Kube layer inputs.
 */
@Singleton
class PayloadKubeInputMapper(
  private val serializer: ObjectSerializer,
  private val labeler: PodLabeler,
  private val orchestratorNameGenerator: OrchestratorNameGenerator,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("orchestratorKubeContainerInfo") private val kubeContainerInfo: KubeContainerInfo,
  @Named("orchestratorEnvMap") private val envMap: Map<String, String>,
  @Named("orchestratorNodeSelectors") private val nodeSelectors: Map<String, String>,
  @Named("orchestratorCustomNodeSelectors") private val customNodeSelectors: Map<String, String>,
) {
  fun toKubeInput(
    input: ReplicationInput,
    workloadId: String,
    passThroughLabels: Map<String, String>,
  ): OrchestratorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val orchestratorPodName = orchestratorNameGenerator.getOrchestratorPodName(jobId, attemptId)

    val orchestratorPodInfo =
      KubePodInfo(
        namespace,
        orchestratorPodName,
        kubeContainerInfo,
      )

    val orchestratorReqs = input.getOrchestratorResourceReqs()
    val nodeSelectors = getNodeSelectors(input)

    val fileMap = buildFileMap(input, input.jobRunConfig, orchestratorPodInfo)

    return OrchestratorKubeInput(
      labeler.getOrchestratorLabels(input, workloadId, passThroughLabels),
      labeler.getSourceLabels(input, workloadId, passThroughLabels),
      labeler.getDestinationLabels(input, workloadId, passThroughLabels),
      nodeSelectors,
      orchestratorPodInfo,
      fileMap,
      orchestratorReqs,
    )
  }

  private fun getNodeSelectors(input: ReplicationInput): Map<String, String> {
    val isCustom = input.usesCustomConnector()

    return if (isCustom) customNodeSelectors else nodeSelectors
  }

  // TODO: This is the way we pass data into the pods we launch. This should be extracted to
  //  some shared interface between parent / child to make it less brittle.
  private fun buildFileMap(
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(envMap),
      OrchestratorConstants.INIT_FILE_APPLICATION to REPLICATION,
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
      OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
      INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
      INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
      KUBE_POD_INFO to serializer.serialize(kubePodInfo),
    )
  }
}

data class OrchestratorKubeInput(
  val orchestratorLabels: Map<String, String>,
  val sourceLabels: Map<String, String>,
  val destinationLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val resourceReqs: ResourceRequirements?,
)
