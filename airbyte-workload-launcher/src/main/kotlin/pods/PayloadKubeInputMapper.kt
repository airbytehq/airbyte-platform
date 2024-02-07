package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.orchestrator.PodNameGenerator
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
import java.util.UUID

/**
 * Maps domain layer objects into Kube layer inputs.
 */
@Singleton
class PayloadKubeInputMapper(
  private val serializer: ObjectSerializer,
  private val labeler: PodLabeler,
  private val podNameGenerator: PodNameGenerator,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("orchestratorKubeContainerInfo") private val orchestratorKubeContainerInfo: KubeContainerInfo,
  @Named("orchestratorEnvMap") private val envMap: Map<String, String>,
  @Named("replicationWorkerConfigs") private val replicationWorkerConfigs: WorkerConfigs,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun toKubeInput(
    workloadId: String,
    input: ReplicationInput,
    sharedLabels: Map<String, String>,
  ): ReplicationOrchestratorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val orchestratorPodName = podNameGenerator.getReplicationOrchestratorPodName(jobId, attemptId)
    val orchestratorImage: String = resolveOrchestratorImageFFOverride(input.connectionId, orchestratorKubeContainerInfo.image)
    val orchestratorPodInfo =
      KubePodInfo(
        namespace,
        orchestratorPodName,
        KubeContainerInfo(orchestratorImage, orchestratorKubeContainerInfo.pullPolicy),
      )

    val orchestratorReqs = input.getOrchestratorResourceReqs()
    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), replicationWorkerConfigs)

    val fileMap = buildFileMap(workloadId, input, input.jobRunConfig, orchestratorPodInfo)

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

  private fun resolveOrchestratorImageFFOverride(
    connectionId: UUID,
    image: String,
  ): String {
    val override = featureFlagClient.stringVariation(ContainerOrchestratorDevImage, Connection(connectionId))
    return override.ifEmpty {
      image
    }
  }

  fun toKubeInput(
    workloadId: String,
    input: CheckConnectionInput,
    sharedLabels: Map<String, String>,
  ): CheckConnectorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val podName = podNameGenerator.getCheckPodName(input.launcherConfig.dockerImage, jobId, attemptId)

    val connectorPodInfo =
      KubePodInfo(
        namespace,
        podName,
        KubeContainerInfo(
          input.launcherConfig.dockerImage,
          checkWorkerConfigs.jobImagePullPolicy,
        ),
      )

    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), checkWorkerConfigs)

    val fileMap = buildFileMap(workloadId, input, input.jobRunConfig)

    return CheckConnectorKubeInput(
      labeler.getCheckConnectorLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
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
    workloadId: String,
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
        OrchestratorConstants.INIT_FILE_APPLICATION to REPLICATION,
        OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(envMap),
        OrchestratorConstants.WORKLOAD_ID_FILE to workloadId,
        INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
        INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
        KUBE_POD_INFO to serializer.serialize(kubePodInfo),
      )
  }

  private fun buildFileMap(
    workloadId: String,
    input: CheckConnectionInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.CONNECTION_CONFIGURATION to serializer.serialize(input.connectionConfiguration.connectionConfiguration),
        OrchestratorConstants.SIDECAR_INPUT to serializer.serialize(SidecarInput(input.connectionConfiguration, workloadId, input.launcherConfig)),
      )
  }

  private fun sharedFileMap(jobRunConfig: JobRunConfig): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
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

data class CheckConnectorKubeInput(
  val connectorLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val annotations: Map<String, String>,
)
