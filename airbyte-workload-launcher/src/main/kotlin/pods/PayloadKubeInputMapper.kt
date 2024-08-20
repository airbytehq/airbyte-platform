package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorSidecarFetchesInputFromInit
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.OrchestratorFetchesInputFromInit
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getAttemptId
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.input.getOrchestratorResourceReqs
import io.airbyte.workers.input.usesCustomConnector
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SidecarInput.OperationType
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workers.pod.PodNameGenerator
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.fabric8.kubernetes.api.model.EnvVar
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

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
  @Named("connectorAwsAssumedRoleSecretEnv") private val connectorAwsAssumedRoleSecretEnvList: List<EnvVar>,
  @Named("replicationWorkerConfigs") private val replicationWorkerConfigs: WorkerConfigs,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  @Named("discoverWorkerConfigs") private val discoverWorkerConfigs: WorkerConfigs,
  @Named("specWorkerConfigs") private val specWorkerConfigs: WorkerConfigs,
  private val featureFlagClient: FeatureFlagClient,
  @Named("infraFlagContexts") private val contexts: List<Context>,
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

    val fileMap = buildSyncFileMap(input)

    val runtimeEnvVars =
      listOf(
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SYNC.toString(), null),
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null),
        EnvVar(AirbyteEnvVar.JOB_ID.toString(), jobId, null),
        EnvVar(AirbyteEnvVar.ATTEMPT_ID.toString(), attemptId.toString(), null),
      )

    return OrchestratorKubeInput(
      labeler.getReplicationOrchestratorLabels(orchestratorKubeContainerInfo.image) + sharedLabels,
      labeler.getSourceLabels() + sharedLabels,
      labeler.getDestinationLabels() + sharedLabels,
      nodeSelectors,
      orchestratorPodInfo,
      fileMap,
      orchestratorReqs,
      replicationWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
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
    logPath: String,
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
      if (WorkloadPriority.DEFAULT == input.launcherConfig.priority) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.launcherConfig.isCustomConnector, checkWorkerConfigs)
      }

    val fileMap = buildCheckFileMap(workloadId, input, logPath)

    val runtimeEnvVars =
      resolveAwsAssumedRoleEnvVars(input.launcherConfig) +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)

    return ConnectorKubeInput(
      labeler.getCheckLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      checkWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
    )
  }

  fun toKubeInput(
    workloadId: String,
    input: DiscoverCatalogInput,
    sharedLabels: Map<String, String>,
    logPath: String,
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
      if (WorkloadPriority.DEFAULT == input.launcherConfig.priority) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.usesCustomConnector(), discoverWorkerConfigs)
      }

    val fileMap = buildDiscoverFileMap(workloadId, input, logPath)

    val runtimeEnvVars =
      resolveAwsAssumedRoleEnvVars(input.launcherConfig) +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)

    return ConnectorKubeInput(
      labeler.getDiscoverLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      discoverWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
    )
  }

  fun toKubeInput(
    workloadId: String,
    input: SpecInput,
    sharedLabels: Map<String, String>,
    logPath: String,
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

    val fileMap = buildSpecFileMap(workloadId, input, logPath)

    val runtimeEnvVars =
      listOf(
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SPEC.toString(), null),
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null),
      )

    return ConnectorKubeInput(
      labeler.getSpecLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      fileMap,
      specWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
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

  private fun buildSyncFileMap(input: ReplicationInput): Map<String, String> {
    val ffContext = Multi(listOf(Connection(input.connectionId), Workspace(input.workspaceId)))

    return buildMap {
      if (!featureFlagClient.boolVariation(OrchestratorFetchesInputFromInit, ffContext)) {
        put(OrchestratorConstants.INIT_FILE_INPUT, serializer.serialize(input))
      }
    }
  }

  private fun buildCheckFileMap(
    workloadId: String,
    input: CheckConnectionInput,
    logPath: String,
  ): Map<String, String> {
    if (featureFlagClient.boolVariation(ConnectorSidecarFetchesInputFromInit, buildFFContext(input.launcherConfig.workspaceId))) {
      return mapOf()
    }

    return mapOf(
      OrchestratorConstants.CONNECTION_CONFIGURATION to serializer.serialize(input.checkConnectionInput.connectionConfiguration),
      OrchestratorConstants.SIDECAR_INPUT to
        serializer.serialize(
          SidecarInput(
            input.checkConnectionInput,
            null,
            workloadId,
            input.launcherConfig,
            OperationType.CHECK,
            logPath,
          ),
        ),
    )
  }

  private fun buildDiscoverFileMap(
    workloadId: String,
    input: DiscoverCatalogInput,
    logPath: String,
  ): Map<String, String> {
    if (featureFlagClient.boolVariation(ConnectorSidecarFetchesInputFromInit, buildFFContext(input.launcherConfig.workspaceId))) {
      return mapOf()
    }

    return mapOf(
      OrchestratorConstants.CONNECTION_CONFIGURATION to serializer.serialize(input.discoverCatalogInput.connectionConfiguration),
      OrchestratorConstants.SIDECAR_INPUT to
        serializer.serialize(
          SidecarInput(
            null,
            input.discoverCatalogInput,
            workloadId,
            input.launcherConfig,
            OperationType.DISCOVER,
            logPath,
          ),
        ),
    )
  }

  private fun buildSpecFileMap(
    workloadId: String,
    input: SpecInput,
    logPath: String,
  ): Map<String, String> {
    if (featureFlagClient.boolVariation(ConnectorSidecarFetchesInputFromInit, buildFFContext(input.launcherConfig.workspaceId))) {
      return mapOf()
    }

    return mapOf(
      OrchestratorConstants.SIDECAR_INPUT to
        serializer.serialize(
          SidecarInput(
            null,
            null,
            workloadId,
            input.launcherConfig,
            OperationType.SPEC,
            logPath,
          ),
        ),
    )
  }

  private fun buildFFContext(workspaceId: UUID): Context {
    return Multi(
      buildList {
        add(Workspace(workspaceId))
        addAll(contexts)
      },
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
  val extraEnv: List<EnvVar>,
)

data class ConnectorKubeInput(
  val connectorLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val fileMap: Map<String, String>,
  val annotations: Map<String, String>,
  val extraEnv: List<EnvVar>,
  val workspaceId: UUID,
)
