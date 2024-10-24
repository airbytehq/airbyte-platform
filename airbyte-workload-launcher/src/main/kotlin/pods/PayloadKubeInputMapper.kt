package io.airbyte.workload.launcher.pods

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.NodeSelectorOverride
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getAttemptId
import io.airbyte.workers.input.getDestinationResourceReqs
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.input.getOrchestratorResourceReqs
import io.airbyte.workers.input.getSourceResourceReqs
import io.airbyte.workers.input.usesCustomConnector
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.KubeContainerInfo
import io.airbyte.workers.pod.KubePodInfo
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workers.pod.PodNameGenerator
import io.airbyte.workers.pod.ResourceConversionUtils
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrganizationId
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

/**
 * Maps domain layer objects into Kube layer inputs.
 */
@Singleton
class PayloadKubeInputMapper(
  private val labeler: PodLabeler,
  private val podNameGenerator: PodNameGenerator,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Named("orchestratorKubeContainerInfo") private val orchestratorKubeContainerInfo: KubeContainerInfo,
  @Named("replicationWorkerConfigs") private val replicationWorkerConfigs: WorkerConfigs,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  @Named("discoverWorkerConfigs") private val discoverWorkerConfigs: WorkerConfigs,
  @Named("specWorkerConfigs") private val specWorkerConfigs: WorkerConfigs,
  @Named("fileTransferReqs") private val fileTransferReqs: AirbyteResourceRequirements,
  private val runTimeEnvVarFactory: RuntimeEnvVarFactory,
  private val featureFlagClient: FeatureFlagClient,
  @Named("infraFlagContexts") private val contexts: List<Context>,
) {
  fun toKubeInput(
    workloadId: String,
    input: ReplicationInput,
    sharedLabels: Map<String, String>,
  ): ReplicationKubeInput {
    val jobId = input.getJobId()
    val attemptId = input.getAttemptId()

    val podName = podNameGenerator.getReplicationPodName(jobId, attemptId)
    val nodeSelectors = getNodeSelectors(input.usesCustomConnector(), replicationWorkerConfigs, input.connectionId)

    val orchImage = resolveOrchestratorImageFFOverride(input.connectionId, orchestratorKubeContainerInfo.image)
    val orchestratorReqs = ResourceConversionUtils.domainToApi(input.getOrchestratorResourceReqs())
    val orchRuntimeEnvVars = runTimeEnvVarFactory.orchestratorEnvVars(input, workloadId)

    val sourceImage = input.sourceLauncherConfig.dockerImage
    val sourceBaseReqs =
      if (input.useFileTransfer) {
        addEphemeralStorageReqLimits(input.getSourceResourceReqs())
      } else {
        input.getSourceResourceReqs()
      }
    val sourceReqs = ResourceConversionUtils.domainToApi(sourceBaseReqs)
    val sourceRuntimeEnvVars = runTimeEnvVarFactory.replicationConnectorEnvVars(input.sourceLauncherConfig, sourceBaseReqs, input.useFileTransfer)

    val destinationImage = input.destinationLauncherConfig.dockerImage
    val destinationReqs = ResourceConversionUtils.domainToApi(input.getDestinationResourceReqs())
    val destinationRuntimeEnvVars =
      runTimeEnvVarFactory.replicationConnectorEnvVars(
        input.destinationLauncherConfig,
        input.getDestinationResourceReqs(),
        input.useFileTransfer,
      )

    val labels =
      labeler.getReplicationLabels(
        orchImage,
        sourceImage,
        destinationImage,
      ) + sharedLabels

    return ReplicationKubeInput(
      podName,
      labels,
      replicationWorkerConfigs.workerKubeAnnotations,
      nodeSelectors,
      orchImage,
      sourceImage,
      destinationImage,
      orchestratorReqs,
      sourceReqs,
      destinationReqs,
      orchRuntimeEnvVars,
      sourceRuntimeEnvVars,
      destinationRuntimeEnvVars,
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
      if (WorkloadPriority.DEFAULT == input.launcherConfig.priority) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.launcherConfig.isCustomConnector, checkWorkerConfigs)
      }

    val runtimeEnvVars = runTimeEnvVarFactory.checkConnectorEnvVars(input.launcherConfig, input.getOrganizationId(), workloadId)

    return ConnectorKubeInput(
      labeler.getCheckLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      checkWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
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
      if (WorkloadPriority.DEFAULT == input.launcherConfig.priority) {
        getNodeSelectors(input.launcherConfig.isCustomConnector, replicationWorkerConfigs)
      } else {
        getNodeSelectors(input.usesCustomConnector(), discoverWorkerConfigs)
      }

    val runtimeEnvVars = runTimeEnvVarFactory.discoverConnectorEnvVars(input.launcherConfig, input.getOrganizationId(), workloadId)

    return ConnectorKubeInput(
      labeler.getDiscoverLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      discoverWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
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

    val runtimeEnvVars = runTimeEnvVarFactory.specConnectorEnvVars(workloadId)

    return ConnectorKubeInput(
      labeler.getSpecLabels() + sharedLabels,
      nodeSelectors,
      connectorPodInfo,
      specWorkerConfigs.workerKubeAnnotations,
      runtimeEnvVars,
      input.launcherConfig.workspaceId,
    )
  }

  private fun getNodeSelectors(
    usesCustomConnector: Boolean,
    workerConfigs: WorkerConfigs,
    connectionId: UUID? = null,
  ): Map<String, String> {
    return if (usesCustomConnector) {
      workerConfigs.workerIsolatedKubeNodeSelectors.orElse(workerConfigs.getworkerKubeNodeSelectors())
    } else {
      getNodeSelectorsOverride(connectionId) ?: workerConfigs.getworkerKubeNodeSelectors()
    }
  }

  private fun getNodeSelectorsOverride(connectionId: UUID?): Map<String, String>? {
    if (contexts.isEmpty() && connectionId == null) {
      return null
    }

    val flagContext = Multi(contexts.toMutableList().also { contextList -> connectionId?.let { contextList.add(Connection(it)) } })
    val nodeSelectorOverride = featureFlagClient.stringVariation(NodeSelectorOverride, flagContext)
    return if (nodeSelectorOverride.isBlank()) {
      null
    } else {
      nodeSelectorOverride.toNodeSelectorMap()
    }
  }

  private fun addEphemeralStorageReqLimits(airbyteSourceReqs: AirbyteResourceRequirements?): AirbyteResourceRequirements {
    return airbyteSourceReqs
      ?.withEphemeralStorageLimit(fileTransferReqs.ephemeralStorageLimit)
      ?.withEphemeralStorageRequest(fileTransferReqs.ephemeralStorageRequest)
      ?: fileTransferReqs
  }
}

data class ReplicationKubeInput(
  val podName: String,
  val labels: Map<String, String>,
  val annotations: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val orchestratorImage: String,
  val sourceImage: String,
  val destinationImage: String,
  val orchestratorReqs: ResourceRequirements,
  val sourceReqs: ResourceRequirements,
  val destinationReqs: ResourceRequirements,
  val orchestratorRuntimeEnvVars: List<EnvVar>,
  val sourceRuntimeEnvVars: List<EnvVar>,
  val destinationRuntimeEnvVars: List<EnvVar>,
)

data class ConnectorKubeInput(
  val connectorLabels: Map<String, String>,
  val nodeSelectors: Map<String, String>,
  val kubePodInfo: KubePodInfo,
  val annotations: Map<String, String>,
  val extraEnv: List<EnvVar>,
  val workspaceId: UUID,
)

@VisibleForTesting
internal fun String.toNodeSelectorMap(): Map<String, String> =
  split(";")
    .associate {
      val (key, value) = it.split("=")
      key.trim() to value.trim()
    }
