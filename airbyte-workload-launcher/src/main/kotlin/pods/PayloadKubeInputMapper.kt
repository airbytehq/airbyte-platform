package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SidecarInput.OperationType
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.orchestrator.PodNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess.KUBE_POD_INFO
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.REPLICATION
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrchestratorResourceReqs
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.airbyte.workload.launcher.serde.ObjectSerializer
import io.fabric8.kubernetes.api.model.EnvVar
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
  private val orchestratorEnvSingleton: OrchestratorEnvSingleton,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("orchestratorKubeContainerInfo") private val orchestratorKubeContainerInfo: KubeContainerInfo,
  @Named("connectorAwsAssumedRoleSecretEnv") private val connectorAwsAssumedRoleSecretEnvList: List<EnvVar>,
  @Named("replicationWorkerConfigs") private val replicationWorkerConfigs: WorkerConfigs,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  @Named("discoverWorkerConfigs") private val discoverWorkerConfigs: WorkerConfigs,
  @Named("specWorkerConfigs") private val specWorkerConfigs: WorkerConfigs,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun toKubeInput(
    workloadId: String,
    input: ReplicationInput,
    sharedLabels: Map<String, String>,
  ): OrchestratorKubeInput {
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

    val fileMap = buildSyncFileMap(workloadId, input, input.jobRunConfig, orchestratorPodInfo)

    return OrchestratorKubeInput(
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
  ): ConnectorKubeInput {
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

    val nodeSelectors =
      if (WorkloadPriority.DEFAULT.equals(input.launcherConfig.priority)) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.launcherConfig.isCustomConnector, checkWorkerConfigs)
      }

    val fileMap = buildCheckFileMap(workloadId, input, input.jobRunConfig)

    val extraEnv = resolveAwsAssumedRoleEnvVars(input.launcherConfig)

    return ConnectorKubeInput(
      labeler.getCheckConnectorLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      checkWorkerConfigs.workerKubeAnnotations,
      extraEnv,
    )
  }

  fun toKubeInput(
    workloadId: String,
    input: DiscoverCatalogInput,
    sharedLabels: Map<String, String>,
  ): ConnectorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val podName = podNameGenerator.getDiscoverPodName(input.launcherConfig.dockerImage, jobId, attemptId)

    val connectorPodInfo =
      KubePodInfo(
        namespace,
        podName,
        KubeContainerInfo(
          input.launcherConfig.dockerImage,
          discoverWorkerConfigs.jobImagePullPolicy,
        ),
      )

    val nodeSelectors =
      if (WorkloadPriority.DEFAULT.equals(input.launcherConfig.priority)) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.usesCustomConnector(), discoverWorkerConfigs)
      }

    val fileMap = buildDiscoverFileMap(workloadId, input, input.jobRunConfig)

    val extraEnv = resolveAwsAssumedRoleEnvVars(input.launcherConfig)

    return ConnectorKubeInput(
      labeler.getCheckConnectorLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      discoverWorkerConfigs.workerKubeAnnotations,
      extraEnv,
    )
  }

  fun toKubeInput(
    workloadId: String,
    input: SpecInput,
    sharedLabels: Map<String, String>,
  ): ConnectorKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val podName = podNameGenerator.getSpecPodName(input.launcherConfig.dockerImage, jobId, attemptId)

    val connectorPodInfo =
      KubePodInfo(
        namespace,
        podName,
        KubeContainerInfo(
          input.launcherConfig.dockerImage,
          specWorkerConfigs.jobImagePullPolicy,
        ),
      )

    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), specWorkerConfigs)

    val fileMap = buildSpecFileMap(workloadId, input, input.jobRunConfig)

    return ConnectorKubeInput(
      labeler.getCheckConnectorLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      specWorkerConfigs.workerKubeAnnotations,
      listOf(),
    )
  }

  private fun resolveAwsAssumedRoleEnvVars(launcherConfig: IntegrationLauncherConfig): List<EnvVar> {
    // Only inject into connectors we own.
    if (launcherConfig.isCustomConnector) {
      return listOf()
    }
    // Only inject into enabled workspaces.
    val workspaceEnabled =
      launcherConfig.workspaceId != null &&
        this.featureFlagClient.boolVariation(InjectAwsSecretsToConnectorPods, Workspace(launcherConfig.workspaceId))
    if (!workspaceEnabled) {
      return listOf()
    }

    val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, launcherConfig.workspaceId.toString(), null)

    return connectorAwsAssumedRoleSecretEnvList + externalIdVar
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
  private fun buildSyncFileMap(
    workloadId: String,
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
    kubePodInfo: KubePodInfo,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
        OrchestratorConstants.INIT_FILE_APPLICATION to REPLICATION,
        OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(orchestratorEnvSingleton.orchestratorEnvMap(input.connectionId)),
        OrchestratorConstants.WORKLOAD_ID_FILE to workloadId,
        INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
        INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
        KUBE_POD_INFO to serializer.serialize(kubePodInfo),
      )
  }

  private fun buildCheckFileMap(
    workloadId: String,
    input: CheckConnectionInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.CONNECTION_CONFIGURATION to serializer.serialize(input.checkConnectionInput.connectionConfiguration),
        OrchestratorConstants.SIDECAR_INPUT to
          serializer.serialize(
            SidecarInput(
              input.checkConnectionInput,
              null,
              workloadId,
              input.launcherConfig,
              OperationType.CHECK,
            ),
          ),
      )
  }

  private fun buildDiscoverFileMap(
    workloadId: String,
    input: DiscoverCatalogInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.CONNECTION_CONFIGURATION to serializer.serialize(input.discoverCatalogInput.connectionConfiguration),
        OrchestratorConstants.SIDECAR_INPUT to
          serializer.serialize(
            SidecarInput(
              null,
              input.discoverCatalogInput,
              workloadId,
              input.launcherConfig,
              OperationType.DISCOVER,
            ),
          ),
      )
  }

  private fun buildSpecFileMap(
    workloadId: String,
    input: SpecInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return sharedFileMap(jobRunConfig) +
      mapOf(
        OrchestratorConstants.SIDECAR_INPUT to
          serializer.serialize(
            SidecarInput(
              null,
              null,
              workloadId,
              input.launcherConfig,
              // TODO: change to OperationType.SPEC once we add it to the sidecar
              OperationType.SPEC,
            ),
          ),
      )
  }

  private fun sharedFileMap(jobRunConfig: JobRunConfig): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
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
  val annotations: Map<String, String>,
)

data class ConnectorKubeInput(
  val connectorLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val annotations: Map<String, String>,
  val extraEnv: List<EnvVar>,
)
