package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.general.DefaultCheckConnectionWorker
import io.airbyte.workers.models.CheckConnectionInput
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
  @Named("replicationWorkerConfigs") private val replicationWorkerConfigs: WorkerConfigs,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  @Named("checkOrchestratorReqs") private val checkOrchestratorReqs: ResourceRequirements,
) {
  fun toKubeInput(
    input: ReplicationInput,
    sharedLabels: Map<String, String>,
  ): ReplicationOrchestratorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val orchestratorPodName = orchestratorNameGenerator.getReplicationOrchestratorPodName(jobId, attemptId)

    val orchestratorPodInfo =
      KubePodInfo(
        namespace,
        orchestratorPodName,
        kubeContainerInfo,
      )

    val orchestratorReqs = input.getOrchestratorResourceReqs()
    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), replicationWorkerConfigs)

    val fileMap = buildFileMap(input, input.jobRunConfig, orchestratorPodInfo)

    return ReplicationOrchestratorKubeInput(
      labeler.getReplicationOrchestratorLabels() + sharedLabels,
      labeler.getSourceLabels() + sharedLabels,
      labeler.getDestinationLabels() + sharedLabels,
      nodeSelectors,
      orchestratorPodInfo,
      fileMap,
      orchestratorReqs,
      replicationWorkerConfigs.workerKubeAnnotations,
    )
  }

  fun toKubeInput(
    input: CheckConnectionInput,
    sharedLabels: Map<String, String>,
  ): CheckOrchestratorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val orchestratorPodName = orchestratorNameGenerator.getCheckOrchestratorPodName(jobId, attemptId)

    val orchestratorPodInfo =
      KubePodInfo(
        namespace,
        orchestratorPodName,
        kubeContainerInfo,
      )

    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), checkWorkerConfigs)

    val fileMap = buildFileMap(input, input.jobRunConfig, orchestratorPodInfo)

    return CheckOrchestratorKubeInput(
      labeler.getCheckOrchestratorLabels() + sharedLabels,
      labeler.getCheckConnectorLabels() + sharedLabels,
      nodeSelectors,
      orchestratorPodInfo,
      fileMap,
      checkOrchestratorReqs,
      checkWorkerConfigs.workerKubeAnnotations,
    )
  }

  private fun getNodeSelectors(
    usesCustomConnector: Boolean,
    workerConfigs: WorkerConfigs,
  ): Map<String, String> {
    return if (usesCustomConnector) {
      workerConfigs.workerIsolatedKubeNodeSelectors.orElse(workerConfigs.getworkerKubeNodeSelectors())
    } else {
      workerConfigs.getworkerKubeNodeSelectors()
    }
  }

  // TODO: This is the way we pass data into the pods we launch. This should be extracted to
  //  some shared interface between parent / child to make it less brittle.
  private fun buildFileMap(
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig, kubePodInfo) +
      mapOf(
        OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
        OrchestratorConstants.INIT_FILE_APPLICATION to REPLICATION,
        INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
        INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
      )
  }

  private fun buildFileMap(
    input: CheckConnectionInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig, kubePodInfo) +
      mapOf(
        OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
        OrchestratorConstants.INIT_FILE_APPLICATION to DefaultCheckConnectionWorker.CHECK,
      )
  }

  private fun sharedFileMap(
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(envMap),
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
      KUBE_POD_INFO to serializer.serialize(kubePodInfo),
    )
  }
}

data class ReplicationOrchestratorKubeInput(
  val orchestratorLabels: Map<String, String>,
  val sourceLabels: Map<String, String>,
  val destinationLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val resourceReqs: ResourceRequirements?,
  val annotations: Map<String, String>,
)

data class CheckOrchestratorKubeInput(
  val orchestratorLabels: Map<String, String>,
  val connectorLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val resourceReqs: ResourceRequirements?,
  val annotations: Map<String, String>,
)
